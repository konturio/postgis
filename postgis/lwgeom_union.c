#include "assert.h"
#include "stddef.h"
#include "string.h"

#include "postgres.h"
#include "fmgr.h"
#include "utils/varlena.h"

#include "../postgis_config.h"

#include "liblwgeom.h"
#include "lwgeom_log.h"
#include "lwgeom_pg.h"
#include "lwgeom_union.h"

/*
 * TODO:
 * * investigate memory management, aggregate context
 *   - no context switch in finalfn? (see pgis_asmvt_finalfn)
 * * why List is used in lwgeom_accum.c instead of LWCOLLECTION?
 */

#define GetAggContext(aggcontext) \
    if (!AggCheckCallContext(fcinfo, aggcontext)) \
        elog(ERROR, "%s called in non-aggregate context", __func__)

#define CheckAggContext() GetAggContext(NULL)

#define ErrorInvalidParam(message) \
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("%s: %s", __func__, message)))


Datum pgis_test_geometry_union_transfn(PG_FUNCTION_ARGS);
Datum pgis_test_geometry_union_combinefn(PG_FUNCTION_ARGS);
Datum pgis_test_geometry_union_serialfn(PG_FUNCTION_ARGS);
Datum pgis_test_geometry_union_deserialfn(PG_FUNCTION_ARGS);
Datum pgis_test_geometry_union_finalfn(PG_FUNCTION_ARGS);

static UnionState* state_create(void);
static void state_init(UnionState *state);
static void state_append(UnionState *state, const LWGEOM *geom);
static bytea* state_serialize(UnionState *state);
static UnionState* state_deserialize(bytea* serialized);
static void state_merge(UnionState *state1, UnionState *state2);

static LWCOLLECTION* partial_union(LWCOLLECTION* col, float8 gridSize);
static void sort_geoms(LWCOLLECTION* col);
static int geom_cmp(const void* g1, const void* g2);

PG_FUNCTION_INFO_V1(pgis_test_geometry_union_transfn);
Datum pgis_test_geometry_union_transfn(PG_FUNCTION_ARGS)
{
    MemoryContext aggcontext = NULL, old;
    UnionState *state;
    Datum argType;
    GSERIALIZED *gser = NULL;

    /* Check argument type */
    argType = get_fn_expr_argtype(fcinfo->flinfo, 1);
    if (argType == InvalidOid)
        ErrorInvalidParam("could not determine input data type");

    /* Get memory context */
    GetAggContext(&aggcontext);

    /* Get state */
    if (PG_ARGISNULL(0)) {
        old = MemoryContextSwitchTo(aggcontext);
        state = state_create();
        MemoryContextSwitchTo(old);
    }
    else
    {
        state = (UnionState*) PG_GETARG_POINTER(0);
    }

    /* Get value */
    if (!PG_ARGISNULL(1))
        gser = PG_GETARG_GSERIALIZED_P(1);

    /* Get grid size */
    if (PG_NARGS() > 2 && !PG_ARGISNULL(2))
    {
        double gridSize = PG_GETARG_FLOAT8(2);
        if (gridSize > 0)
            state->gridSize = gridSize;
    }

    /* Copy geometry into state */
    if (gser) {
        LWGEOM *geom;
        old = MemoryContextSwitchTo(aggcontext);

        geom = lwgeom_from_gserialized(gser);
        state_append(state, lwgeom_clone_deep(geom));
        lwgeom_free(geom);

        MemoryContextSwitchTo(old);
    }

    PG_RETURN_POINTER(state);
}


PG_FUNCTION_INFO_V1(pgis_test_geometry_union_combinefn);
Datum pgis_test_geometry_union_combinefn(PG_FUNCTION_ARGS)
{
    MemoryContext aggcontext = NULL, old;
    UnionState *state1;
    UnionState *state2;

    POSTGIS_DEBUG(1, "pgis_test_geometry_union_combinefn");

    state1 = (UnionState*) PG_GETARG_POINTER(0);
    state2 = (UnionState*) PG_GETARG_POINTER(1);

    POSTGIS_DEBUGF(1, "  # of geoms: %d", (state1 && state1->geoms) ? state1->geoms->ngeoms : 0);
    POSTGIS_DEBUGF(1, "  # of geoms: %d", (state2 && state2->geoms) ? state2->geoms->ngeoms : 0);

    GetAggContext(&aggcontext);
    old = MemoryContextSwitchTo(aggcontext);

    /* Merge states */
    if (state1 && state2) {
        state_merge(state1, state2);
        lwfree(state2);
        state2 = NULL;
    }
    else if (state2)
    {
        state1 = state2;
    }

    MemoryContextSwitchTo(old);

    /* Mark result as merged */
    if (state1)
        state1->isMerged = true;

    PG_RETURN_POINTER(state1);
}


PG_FUNCTION_INFO_V1(pgis_test_geometry_union_serialfn);
Datum pgis_test_geometry_union_serialfn(PG_FUNCTION_ARGS)
{
    MemoryContext aggcontext = NULL, old;
    UnionState *state;
    bytea *serialized;

    POSTGIS_DEBUG(1, "pgis_test_geometry_union_serialfn");

    GetAggContext(&aggcontext);
    old = MemoryContextSwitchTo(aggcontext);

    /* TODO: don't create new state, just pass NULL */
    if (!PG_ARGISNULL(0))
        state = (UnionState*) PG_GETARG_POINTER(0);
    else
        state = state_create();

    serialized = state_serialize(state);

    MemoryContextSwitchTo(old);

    PG_RETURN_BYTEA_P(serialized);
}


PG_FUNCTION_INFO_V1(pgis_test_geometry_union_deserialfn);
Datum pgis_test_geometry_union_deserialfn(PG_FUNCTION_ARGS)
{
    MemoryContext aggcontext = NULL, old;
    UnionState *state;
    bytea *serialized;

    POSTGIS_DEBUG(1, "pgis_test_geometry_union_deserialfn");

    if (PG_ARGISNULL(0))
        ErrorInvalidParam("Empty serialized state value");
    serialized = (bytea*)PG_GETARG_POINTER(0);

    GetAggContext(&aggcontext);
    old = MemoryContextSwitchTo(aggcontext);

    state = state_deserialize(serialized);
    state->isMerged = true;

    MemoryContextSwitchTo(old);

    PG_RETURN_POINTER(state);
}


PG_FUNCTION_INFO_V1(pgis_test_geometry_union_finalfn);
Datum pgis_test_geometry_union_finalfn(PG_FUNCTION_ARGS)
{
    MemoryContext aggcontext = NULL, old;
    UnionState *state = NULL;
    GSERIALIZED* result = NULL;
    LWGEOM* geom;

    POSTGIS_DEBUG(1, "pgis_test_geometry_union_finalfn");

    if (PG_ARGISNULL(0))
        ErrorInvalidParam("Empty state value");
    state = (UnionState*)PG_GETARG_POINTER(0);

    POSTGIS_DEBUGF(1, "  # of geoms: %d", (state && state->geoms) ? state->geoms->ngeoms : 0);

    /* TODO: return empty_type, see lwgeom_geos.c */
    if (!state->geoms)
        PG_RETURN_NULL();

    GetAggContext(&aggcontext);
    old = MemoryContextSwitchTo(aggcontext);

    POSTGIS_DEBUGF(1, " grid size: %f", (float) state->gridSize);

    geom = lwgeom_unaryunion_prec(lwcollection_as_lwgeom(state->geoms), state->gridSize);
    result = geometry_serialize(geom);
    lwgeom_free(geom);

    MemoryContextSwitchTo(old);

    PG_RETURN_POINTER(result);
}


UnionState* state_create(void)
{
    UnionState *state = lwalloc(sizeof(UnionState));
    state_init(state);
    return state;
}


void state_init(UnionState *state)
{
    state->geoms = NULL;
    state->gridSize = -1.0;
    state->isMerged = false;
}


void state_append(UnionState *state, const LWGEOM *geom)
{
    if (!state->geoms)
    {
        int32_t srid = lwgeom_get_srid(geom);
        int hasz = FLAGS_GET_Z(geom->flags);
        int hasm = FLAGS_GET_M(geom->flags);
        state->geoms = lwcollection_construct_empty(COLLECTIONTYPE, srid, hasz, hasm);
    }

    state->geoms = lwcollection_add_lwgeom(state->geoms, geom);
}


bytea* state_serialize(UnionState *state)
{
    GSERIALIZED *serialized_geoms = NULL;
    bytea *serialized;
    uint8 *data;
    int32 size = VARHDRSZ + sizeof(state->gridSize);

    if (state->geoms)
    {
        if (!state->isMerged)
        {
            sort_geoms(state->geoms);
            state->geoms = partial_union(state->geoms, state->gridSize);
        }

        serialized_geoms = geometry_serialize(lwcollection_as_lwgeom(state->geoms));
        size += VARSIZE(serialized_geoms);
    }

    serialized = lwalloc(size);
    SET_VARSIZE(serialized, size);
    data = (uint8*)VARDATA(serialized);

    /* grid size */
    memcpy(data, &state->gridSize, sizeof(state->gridSize));
    data += sizeof(state->gridSize);

    /* geometry collection */
    if (serialized_geoms)
    {
        memcpy(data, serialized_geoms, VARSIZE(serialized_geoms));
        lwfree(serialized_geoms);
    }

    return serialized;
}


UnionState* state_deserialize(bytea* serialized)
{
    UnionState *state = state_create();
    uint8 *data = (uint8*)VARDATA(serialized);

    /* grid size */
    memcpy(&state->gridSize, data, sizeof(state->gridSize));
    data += sizeof(state->gridSize);

    /* geometry collection */
    if (VARSIZE(serialized) - VARHDRSZ > sizeof(state->gridSize))
    {
        GSERIALIZED *serialized_geoms;
        LWGEOM *geom;

        assert(VARSIZE(serialized) > VARHDRSZ * 2 + sizeof(state->gridSize));

        /* Deserialize */
        serialized_geoms = (GSERIALIZED*)data;
        geom = lwgeom_from_gserialized(serialized_geoms);

        /* Copy collection */
        state->geoms = lwgeom_as_lwcollection(lwgeom_clone_deep(geom));

        /* Cleanup */
        lwgeom_release(geom);

        assert(state->geoms);
    }

    return state;
}


void state_merge(UnionState *state1, UnionState *state2)
{
    LWCOLLECTION *geoms1 = state1->geoms;
    LWCOLLECTION *geoms2 = state2->geoms;

    if (geoms1 && geoms2)
    {
        state1->geoms = lwcollection_concat_in_place(geoms1, geoms2);
        lwcollection_release(state2->geoms);
    }
    else
    {
        state1->geoms = geoms1 ? geoms1 : geoms2;
    }

    state2->geoms = NULL;
}


/* NOTE: original collection will be released */
LWCOLLECTION* partial_union(LWCOLLECTION* col, float8 gridSize)
{
    LWCOLLECTION *result = NULL;
    uint32_t j = 0; /* start index of sequence of geoms with overlapping boxes */
    GBOX *bbox = NULL; /* merged bbox */

    /* TODO: add unique identifier */
    POSTGIS_DEBUG(1, "  partial_union");
    POSTGIS_DEBUGF(1, "    # of geoms: %d", col->ngeoms);

    result = lwcollection_construct_empty(
        col->type,
        col->srid,
        FLAGS_GET_Z(col->flags),
        FLAGS_GET_M(col->flags));

    for (uint32_t i = 0; i < col->ngeoms + 1; ++i) {
        LWGEOM *cur = NULL;
        const GBOX *cur_bbox = NULL;

        /* Get current geom */
        if (i < col->ngeoms)
        {
            cur = col->geoms[i];
            cur_bbox = lwgeom_get_bbox(cur); /* can be NULL for empty LWGEOM */
        }

        /* NOTE: merging empty geoms */
        if (i > 0 && (!cur || (bbox && cur_bbox && !gbox_overlaps(bbox, cur_bbox))))
        {
            /* Add sequence union to result */
            if (i - j > 1)
            {
                LWCOLLECTION *aux;
                LWGEOM *merged;
                POSTGIS_DEBUGF(1, "    (merging %d geoms)", i - j);

                /* Create union */
                aux = lwcollection_construct(
                    col->type,
                    col->srid,
                    NULL,
                    i - j,
                    &col->geoms[j]);
                merged = lwgeom_unaryunion_prec(lwcollection_as_lwgeom(aux), gridSize);
                lwcollection_release(aux);

                /* Append to result */
                if (lwgeom_is_collection(merged))
                    result = lwcollection_concat_in_place(result, lwgeom_as_lwcollection(merged));
                else
                    result = lwcollection_add_lwgeom(result, merged);

                /* Free used geoms */
                for (uint32_t k = j; k < i; ++k) {
                    lwgeom_free(col->geoms[k]);
                    col->geoms[k] = NULL;
                }
            }
            else
            {
                result = lwcollection_add_lwgeom(result, col->geoms[j]);
            }

            if (cur)
            {
                /* Start next sequence */
                j = i;
                bbox = cur_bbox ? gbox_copy(cur_bbox) : NULL;
            }
        }
        else if (cur && cur_bbox)
        {
            /* Init or update bbox for current sequence */
            if (bbox)
                gbox_merge(cur_bbox, bbox);
            else
                bbox = gbox_copy(cur_bbox);
        }
    }

    POSTGIS_DEBUGF(1, "    # of geoms after union: %d", result->ngeoms);

    lwcollection_release(col);
    return result;
}


void sort_geoms(LWCOLLECTION* col)
{
    qsort(col->geoms, col->ngeoms, sizeof(LWGEOM*), &geom_cmp);
}


int geom_cmp(const void *_g1, const void *_g2)
{
    const LWGEOM *g1 = _g1;
    const LWGEOM *g2 = _g2;
    const GBOX *bbox1 = lwgeom_get_bbox(g1);
    const GBOX *bbox2 = lwgeom_get_bbox(g2);
    uint64_t hash1, hash2;

    /* Ignore empty boxes */
    if (!bbox1 || !bbox2)
        return 0;

    hash1 = gbox_get_sortable_hash(bbox1, g1->srid);
    hash2 = gbox_get_sortable_hash(bbox2, g2->srid);

    if (hash1 > hash2)
        return 1;
    if (hash1 < hash2)
        return -1;
    return 0;
}
