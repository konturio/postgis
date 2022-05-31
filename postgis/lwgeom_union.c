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
Datum pgis_test_geometry_union_deeserialfn(PG_FUNCTION_ARGS);
Datum pgis_test_geometry_union_finalfn(PG_FUNCTION_ARGS);

static UnionState* state_create(void);
static void state_append(UnionState *state, const GSERIALIZED *gser);
static bytea* state_serialize(const UnionState *state);
static UnionState* state_deserialize(const bytea* serialized);
static void state_combine(UnionState *state1, UnionState *state2);
static int state_geom_num(const UnionState *state);

static LWCOLLECTION* lwcollection_from_gserialized_list(List* list);


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

    /* Copy serialized geometry into state */
    if (gser) {
        old = MemoryContextSwitchTo(aggcontext);
        state_append(state, gser);
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

    POSTGIS_DEBUGF(1, "  # of geoms: %d", state_geom_num(state1));
    POSTGIS_DEBUGF(1, "  # of geoms: %d", state_geom_num(state2));

    GetAggContext(&aggcontext);
    old = MemoryContextSwitchTo(aggcontext);

    /* Combine states */
    if (state1 && state2)
    {
        state_combine(state1, state2);
        lwfree(state2);
    }
    else if (state2)
    {
        state1 = state2;
    }

    MemoryContextSwitchTo(old);

    PG_RETURN_POINTER(state1);
}


PG_FUNCTION_INFO_V1(pgis_test_geometry_union_serialfn);
Datum pgis_test_geometry_union_serialfn(PG_FUNCTION_ARGS)
{
    MemoryContext aggcontext = NULL, old;
    UnionState *state;
    bytea *serialized = NULL;

    POSTGIS_DEBUG(1, "pgis_test_geometry_union_serialfn");

    GetAggContext(&aggcontext);
    old = MemoryContextSwitchTo(aggcontext);

    if (!PG_ARGISNULL(0))
    {
        state = (UnionState*) PG_GETARG_POINTER(0);
    }
    else
    {
        state = state_create();
    }
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

    MemoryContextSwitchTo(old);

    PG_RETURN_POINTER(state);
}


PG_FUNCTION_INFO_V1(pgis_test_geometry_union_finalfn);
Datum pgis_test_geometry_union_finalfn(PG_FUNCTION_ARGS)
{
    MemoryContext aggcontext = NULL, old;
    UnionState *state;
    LWCOLLECTION *col;
    GSERIALIZED *result;
    LWGEOM *geom;

    POSTGIS_DEBUG(1, "pgis_test_geometry_union_finalfn");

    if (PG_ARGISNULL(0))
        ErrorInvalidParam("Empty state value");
    state = (UnionState*)PG_GETARG_POINTER(0);

    POSTGIS_DEBUGF(1, "  # of geoms: %d", state_geom_num(state));
    POSTGIS_DEBUGF(1, " grid size: %f", (float) state->gridSize);

    GetAggContext(&aggcontext);
    old = MemoryContextSwitchTo(aggcontext);

    col = lwcollection_from_gserialized_list(state->list);
    geom = lwgeom_unaryunion_prec(lwcollection_as_lwgeom(col), state->gridSize);
    if (geom)
    {
        result = geometry_serialize(geom);
        lwgeom_free(geom);
    }

    /* TODO: clean list and collection immetiately? */

    MemoryContextSwitchTo(old);

    PG_RETURN_POINTER(result);
}


UnionState* state_create(void)
{
    UnionState *state = lwalloc(sizeof(UnionState));
    state->gridSize = -1.0;
    state->list = NIL;
    state->size = 0;
    return state;
}


void state_append(UnionState *state, const GSERIALIZED *gser)
{
    GSERIALIZED *gser_copy;

    assert(gser);
    gser_copy = lwalloc(VARSIZE(gser));
    memcpy(gser_copy, gser, VARSIZE(gser));

    state->list = lappend(state->list, gser_copy);
    state->size += VARSIZE(gser);
}


bytea* state_serialize(const UnionState *state)
{
    int32 size = VARHDRSZ + sizeof(state->gridSize) + state->size;
    bytea *serialized = lwalloc(size);
    uint8 *data;
    ListCell *cell;

    SET_VARSIZE(serialized, size);
    data = (uint8*)VARDATA(serialized);

    /* grid size */
    memcpy(data, &state->gridSize, sizeof(state->gridSize));
    data += sizeof(state->gridSize);

    /* items */
    foreach (cell, state->list)
    {
        const GSERIALIZED *gser = (const GSERIALIZED*)lfirst(cell);
        assert(gser);
        memcpy(data, gser, VARSIZE(gser));
        data += VARSIZE(gser);
    }

    return serialized;
}


UnionState* state_deserialize(const bytea* serialized)
{
    UnionState *state = state_create();
    const uint8 *data = (const uint8*)VARDATA(serialized);
    const uint8 *data_end = (const uint8*)serialized + VARSIZE(serialized);

    /* grid size */
    memcpy(&state->gridSize, data, sizeof(state->gridSize));
    data += sizeof(state->gridSize);

    /* items */
    while (data < data_end)
    {
        const GSERIALIZED* gser = (const GSERIALIZED*)data;
        state_append(state, gser);
        data += VARSIZE(gser);
    }

    return state;
}


void state_combine(UnionState *state1, UnionState *state2)
{
    List *list1 = state1->list;
    List *list2 = state2->list;

    if (list1 != NIL && list2 != NIL)
    {
        state1->list = list_concat(list1, list2);
        list_free(list2);
    }
    else if (list2 != NIL)
    {
        state1->list = list2;
    }
    state2->list = NIL;
}


int state_geom_num(const UnionState *state)
{
    if (!state) return -1;
    return (state->list != NIL) ? state->list->length : 0;
}


LWCOLLECTION* lwcollection_from_gserialized_list(List* list)
{
    int ngeoms;
    LWGEOM **geoms;
    int32_t srid;
    ListCell *cell;
    int i;

    if (list == NIL)
        return NULL;

    ngeoms = list->length;
    assert(ngeoms > 0);

    geoms = lwalloc(ngeoms * sizeof(LWGEOM*));
    srid = SRID_UNKNOWN;
    i = 0;
    foreach (cell, list)
    {
        int32_t geom_srid;
        GSERIALIZED *gser;
        LWGEOM *geom;

        gser = (GSERIALIZED*)lfirst(cell);
        assert(gser);
        geom = lwgeom_from_gserialized(gser);

        geom_srid = lwgeom_get_srid(geom);
        if (srid == SRID_UNKNOWN && geom_srid != SRID_UNKNOWN)
            srid = geom_srid;

        geoms[i++] = geom; /* no cloning */
    }
    assert(i == ngeoms);

    return lwcollection_construct(COLLECTIONTYPE, srid, NULL, ngeoms, geoms);
}
