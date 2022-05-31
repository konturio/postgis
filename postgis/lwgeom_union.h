#ifndef _LWGEOM_ACCUM_H
#define _LWGEOM_ACCUM_H 1

#include <stdbool.h>
#include "postgres.h"
#include "liblwgeom.h"

typedef struct UnionState
{
    float8 gridSize;
    List *list;
    int32 size;
} UnionState;

#endif
