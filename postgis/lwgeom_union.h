#ifndef _LWGEOM_ACCUM_H
#define _LWGEOM_ACCUM_H 1

#include <stdbool.h>
#include "postgres.h"
#include "liblwgeom.h"

typedef struct UnionState
{
    LWCOLLECTION *geoms;
    float8 gridSize;
    bool isMerged;
} UnionState;

#endif
