/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import io.airlift.slice.Slice;

@ScalarFunction("map_top_n_values")
@Description("Get the top N values of the given map using a lambda comparator.")
public final class MapTopNValuesComparatorFunction
{
    @TypeParameter("K")
    @TypeParameter("V")
    public MapTopNValuesComparatorFunction(@TypeParameter("K") Type keyType, @TypeParameter("V") Type valueType) {}

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "V", nativeContainerType = long.class)
    @SqlType("array(V)")
    public Block topNValuesLong(
            @TypeParameter("V") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(V, V, int)") ArraySortComparatorFunction.ComparatorLongLambda function)
    {
        if (n == 0) {
            return type.createBlockBuilder(null, 0).build();
        }
        ArraySortComparatorFunction instance = new ArraySortComparatorFunction(type);
        Block block = instance.sortLong(type, MapValues.getValues(type, mapBlock), function);

        return MapTopNValuesFunction.computeTopNBlock(type, block, n);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "V", nativeContainerType = double.class)
    @SqlType("array(V)")
    public Block topNValuesDouble(
            @TypeParameter("V") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(V, V, int)") ArraySortComparatorFunction.ComparatorDoubleLambda function)
    {
        if (n == 0) {
            return type.createBlockBuilder(null, 0).build();
        }
        ArraySortComparatorFunction instance = new ArraySortComparatorFunction(type);
        Block block = instance.sortDouble(type, MapValues.getValues(type, mapBlock), function);

        return MapTopNValuesFunction.computeTopNBlock(type, block, n);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "V", nativeContainerType = boolean.class)
    @SqlType("array(V)")
    public Block topNValuesBoolean(
            @TypeParameter("V") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(V, V, int)") ArraySortComparatorFunction.ComparatorBooleanLambda function)
    {
        if (n == 0) {
            return type.createBlockBuilder(null, 0).build();
        }
        ArraySortComparatorFunction instance = new ArraySortComparatorFunction(type);
        Block block = instance.sortBoolean(type, MapValues.getValues(type, mapBlock), function);

        return MapTopNValuesFunction.computeTopNBlock(type, block, n);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "V", nativeContainerType = Slice.class)
    @SqlType("array(V)")
    public Block topNValuesSlice(
            @TypeParameter("V") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(V, V, int)") ArraySortComparatorFunction.ComparatorSliceLambda function)
    {
        if (n == 0) {
            return type.createBlockBuilder(null, 0).build();
        }
        ArraySortComparatorFunction instance = new ArraySortComparatorFunction(type);
        Block block = instance.sortSlice(type, MapValues.getValues(type, mapBlock), function);

        return MapTopNValuesFunction.computeTopNBlock(type, block, n);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "V", nativeContainerType = Block.class)
    @SqlType("array(V)")
    public Block topNValuesObject(
            @TypeParameter("V") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(V, V, int)") ArraySortComparatorFunction.ComparatorBlockLambda function)
    {
        if (n == 0) {
            return type.createBlockBuilder(null, 0).build();
        }
        ArraySortComparatorFunction instance = new ArraySortComparatorFunction(type);
        Block block = instance.sortObject(type, MapValues.getValues(type, mapBlock), function);

        return MapTopNValuesFunction.computeTopNBlock(type, block, n);
    }
}
