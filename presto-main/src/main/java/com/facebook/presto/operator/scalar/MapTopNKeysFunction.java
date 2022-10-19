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
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.LESS_THAN;

@ScalarFunction("map_top_n_keys")
@Description("Gives the top N values of the given map in descending order according to the natural ordering of its values.")
public final class MapTopNKeysFunction
{
    private MapTopNKeysFunction() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType("array(K)")
    public static Block topNKeys(
            @OperatorDependency(operator = LESS_THAN, argumentTypes = {"V", "V"}) MethodHandle lessThanFunction,
            @TypeParameter("K") Type type,
            @SqlType("map(K,V)") Block mapBlock,
            @SqlType(StandardTypes.BIGINT) long n)
    {
        if (n <= 0) {
            //TODO: We may wanna throw an error for negative N
            return type.createBlockBuilder(null, 0).build();
        }

        Block block = ArraySortFunction.sort(lessThanFunction, type, MapKeys.getKeys(type, mapBlock));
        return computeTopNBlock(type, block, n);
    }

    public static Block computeTopNBlock(Type type, Block sortedBlock, long n)
    {
        int arrayLength = sortedBlock.getPositionCount();
        BlockBuilder blockBuilder = type.createBlockBuilder(null, arrayLength);

        long cnt = 0;
        for (int i = arrayLength - 1; i >= 0 && cnt < n; --i) {
            if (sortedBlock.isNull(i)) {
                continue;
            }

            type.appendTo(sortedBlock, i, blockBuilder);
            cnt++;
        }

        return blockBuilder.build();
    }
}
