#include <postgres.h>
#include "fmgr.h"

#include <funcapi.h>
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "nodes/pg_list.h"

extern Datum hll_empty4(PG_FUNCTION_ARGS);
extern Datum hll_union(PG_FUNCTION_ARGS);

static List * unpack_conv_data(ArrayType *conv_data);
static List * unpack_goal_types(ArrayType *goal_types);
static List * filter_conv_data(List *conv_data_list, List *goal_type_list);

PG_FUNCTION_INFO_V1(sum_conv_action_count);
Datum sum_conv_action_count(PG_FUNCTION_ARGS);
Datum
sum_conv_action_count(PG_FUNCTION_ARGS)
{
    ArrayType *conv_data = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *goal_types = PG_GETARG_ARRAYTYPE_P(1);

    List *conv_data_list = unpack_conv_data(conv_data);
    List *goal_type_list = unpack_goal_types(goal_types);

    ListCell *conv_data_cell = NULL;
    int64 result = 0;

    conv_data_list = filter_conv_data(conv_data_list, goal_type_list);

    foreach(conv_data_cell, conv_data_list)
    {
        Datum *values = lfirst(conv_data_cell);
        int64 action_count = DatumGetInt64(values[3]);
        result += action_count;
    }

    PG_RETURN_INT64(result);
}

PG_FUNCTION_INFO_V1(union_conv_action_count);
Datum union_conv_action_count(PG_FUNCTION_ARGS);
Datum
union_conv_action_count(PG_FUNCTION_ARGS)
{
    ArrayType *conv_data = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *goal_types = PG_GETARG_ARRAYTYPE_P(1);

    List *conv_data_list = unpack_conv_data(conv_data);
    List *goal_type_list = unpack_goal_types(goal_types);

    ListCell *conv_data_cell = NULL;
    Datum result = DirectFunctionCall4(hll_empty4,
                                       Int32GetDatum(12),
                                       Int32GetDatum(5),
                                       Int32GetDatum(8),
                                       Int32GetDatum(0));

    conv_data_list = filter_conv_data(conv_data_list, goal_type_list);

    foreach(conv_data_cell, conv_data_list)
    {
        Datum *values = lfirst(conv_data_cell);
        Datum action_uu_count = values[4];
        
        result = DirectFunctionCall2(hll_union, result, action_uu_count);
    }

    PG_RETURN_DATUM(result);
}

static List *
unpack_conv_data(ArrayType *conv_data)
{
    List *result = NIL;
    Datum *conv_data_array = NULL;
    int conv_data_count = 0;
    int i = 0;

    Oid convOid = ARR_ELEMTYPE(conv_data);
    TupleDesc convTupleDesc = TypeGetTupleDesc(convOid, NIL);

    deconstruct_array(conv_data, convOid, -1, false, 'i',
                      &conv_data_array, NULL, &conv_data_count);

    for (i = 0; i < conv_data_count; i++)
    {
        Datum *values = NULL;
        bool *nulls = NULL;

        HeapTupleHeader tupleHeader = DatumGetHeapTupleHeader(conv_data_array[i]);
        HeapTupleData tuple = {
            .t_len = HeapTupleHeaderGetDatumLength(tupleHeader),
            .t_tableOid = InvalidOid,
            .t_data = tupleHeader
        };
        ItemPointerSetInvalid(&tuple.t_self);

        values = palloc(6 * sizeof(Datum));
        nulls = palloc(6 * sizeof(bool));
        heap_deform_tuple(&tuple, convTupleDesc, values, nulls);

        result = lappend(result, values);
    }

    return result;
}

static List *
unpack_goal_types(ArrayType *goal_types)
{
    List *result = NIL;
    Datum *goal_type_array = NULL;
    int goal_type_count = 0;
    int i = 0;

    deconstruct_array(goal_types, TEXTOID, -1, false, 'i',
                      &goal_type_array, NULL, &goal_type_count);

    for (i = 0; i < goal_type_count; i++)
    {
        char *goal_type = text_to_cstring(DatumGetTextP(goal_type_array[i]));
        result = lappend(result, goal_type);
    }

    return result;
}

static List *
filter_conv_data(List *conv_data_list, List *goal_type_list)
{
    List *result = NIL;
    ListCell *conv_data_cell = NULL;
    ListCell *goal_type_cell = NULL;

    foreach(conv_data_cell, conv_data_list)
    {
        Datum *values = lfirst(conv_data_cell);
        const char *conversion_type = text_to_cstring(DatumGetTextP(values[1]));

        foreach(goal_type_cell, goal_type_list)
        {
            const char *goal_type = lfirst(goal_type_cell);
            if (strcmp(conversion_type, goal_type) == 0)
            {
                result = lappend(result, values);
                break;
            }
        }
    }

    return result;
}
