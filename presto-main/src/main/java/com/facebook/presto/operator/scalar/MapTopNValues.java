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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import com.facebook.presto.sql.gen.lambda.LambdaFunctionInterface;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import java.util.Comparator;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;

@ScalarFunction("map_top_n_values")
@Description("Sorts the given array with a lambda comparator.")
public final class MapTopNValues
{
    private static final int INITIAL_LENGTH = 128;
    private List<Integer> positions = Ints.asList(new int[INITIAL_LENGTH]);

    @TypeParameter("K")
    @TypeParameter("V")
    public MapTopNValues(@TypeParameter("K") Type keyType, @TypeParameter("V") Type valueType) {}

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "V", nativeContainerType = long.class)
    @SqlType("array(V)")
    public Block topNValuesLong(
            @TypeParameter("V") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(V, V, int)") ComparatorLongLambda function)
    {
        Block block = MapValues.getValues(type, mapBlock);
        int arrayLength = block.getPositionCount();
        initPositionsList(arrayLength);

        Comparator<Integer> comparator = (x, y) -> comparatorResult(function.apply(
                block.isNull(x) ? null : type.getLong(block, x),
                block.isNull(y) ? null : type.getLong(block, y)));

        sortPositions(arrayLength, comparator);

        return computeResultBlock(type, block, arrayLength, n);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "V", nativeContainerType = double.class)
    @SqlType("array(V)")
    public Block topNValuesDouble(
            @TypeParameter("V") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(V, V, int)") ComparatorDoubleLambda function)
    {
        Block block = MapValues.getValues(type, mapBlock);
        int arrayLength = block.getPositionCount();
        initPositionsList(arrayLength);

        Comparator<Integer> comparator = (x, y) -> comparatorResult(function.apply(
                block.isNull(x) ? null : type.getDouble(block, x),
                block.isNull(y) ? null : type.getDouble(block, y)));

        sortPositions(arrayLength, comparator);

        return computeResultBlock(type, block, arrayLength, n);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "V", nativeContainerType = boolean.class)
    @SqlType("array(V)")
    public Block topNValuesBoolean(
            @TypeParameter("V") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(V, V, int)") ComparatorBooleanLambda function)
    {
        Block block = MapValues.getValues(type, mapBlock);
        int arrayLength = block.getPositionCount();
        initPositionsList(arrayLength);

        Comparator<Integer> comparator = (x, y) -> comparatorResult(function.apply(
                block.isNull(x) ? null : type.getBoolean(block, x),
                block.isNull(y) ? null : type.getBoolean(block, y)));

        sortPositions(arrayLength, comparator);

        return computeResultBlock(type, block, arrayLength, n);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "V", nativeContainerType = Slice.class)
    @SqlType("array(V)")
    public Block topNValuesSlice(
            @TypeParameter("V") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(V, V, int)") ComparatorSliceLambda function)
    {
        Block block = MapValues.getValues(type, mapBlock);
        int arrayLength = block.getPositionCount();
        initPositionsList(arrayLength);

        Comparator<Integer> comparator = (x, y) -> comparatorResult(function.apply(
                block.isNull(x) ? null : type.getSlice(block, x),
                block.isNull(y) ? null : type.getSlice(block, y)));

        sortPositions(arrayLength, comparator);

        return computeResultBlock(type, block, arrayLength, n);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @TypeParameterSpecialization(name = "V", nativeContainerType = Block.class)
    @SqlType("array(V)")
    public Block topNValuesObject(
            @TypeParameter("V") Type type,
            @SqlType("map(K, V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n,
            @SqlType("function(V, V, int)") ComparatorBlockLambda function)
    {
        Block block = MapValues.getValues(type, mapBlock);
        int arrayLength = block.getPositionCount();
        initPositionsList(arrayLength);

        Comparator<Integer> comparator = (x, y) -> comparatorResult(function.apply(
                block.isNull(x) ? null : (Block) type.getObject(block, x),
                block.isNull(y) ? null : (Block) type.getObject(block, y)));

        sortPositions(arrayLength, comparator);

        return computeResultBlock(type, block, arrayLength, n);
    }

    private void initPositionsList(int arrayLength)
    {
        if (positions.size() < arrayLength) {
            positions = Ints.asList(new int[arrayLength]);
        }
        for (int i = 0; i < arrayLength; i++) {
            positions.set(i, i);
        }
    }

    private void sortPositions(int arrayLength, Comparator<Integer> comparator)
    {
        List<Integer> list = positions.subList(0, arrayLength);

        try {
            list.sort(comparator);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Lambda comparator violates the comparator contract", e);
        }
    }

    private Block computeResultBlock(Type type, Block block, int arrayLength, long n)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, arrayLength);

        long cnt = 0;
        for (int i = arrayLength - 1; i >= 0 && cnt < n; --i) {
            type.appendTo(block, positions.get(i), blockBuilder);
            cnt++;
        }

        return blockBuilder.build();
    }

    private static int comparatorResult(Long result)
    {
        checkCondition(
                (result != null) && ((result == -1) || (result == 0) || (result == 1)),
                INVALID_FUNCTION_ARGUMENT,
                "Lambda comparator must return either -1, 0, or 1");
        return result.intValue();
    }

    @FunctionalInterface
    public interface ComparatorLongLambda
            extends LambdaFunctionInterface
    {
        Long apply(Long x, Long y);
    }

    @FunctionalInterface
    public interface ComparatorDoubleLambda
            extends LambdaFunctionInterface
    {
        Long apply(Double x, Double y);
    }

    @FunctionalInterface
    public interface ComparatorBooleanLambda
            extends LambdaFunctionInterface
    {
        Long apply(Boolean x, Boolean y);
    }

    @FunctionalInterface
    public interface ComparatorSliceLambda
            extends LambdaFunctionInterface
    {
        Long apply(Slice x, Slice y);
    }

    @FunctionalInterface
    public interface ComparatorBlockLambda
            extends LambdaFunctionInterface
    {
        Long apply(Block x, Block y);
    }
}
