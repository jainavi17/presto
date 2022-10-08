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

import com.facebook.presto.common.type.ArrayType;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;

public class TestMapTopNValues
        extends AbstractTestFunctions
{
    @Test
    public void testBasic()
    {
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 2, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList(1, 2));
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 2, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList(3, 2));
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY['x', 'y', 'z'], ARRAY['a', 'b', 'c']), 3, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(1)), asList("c", "b", "a"));
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY['x', 'y', 'z'], ARRAY['a1', 'b2', 'c3']), 1, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(2)), asList("c3"));
    }

    @Test
    public void testNLargerThanMapSize()
    {
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 8, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList(1, 2, 3));
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), 9, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList(3, 2, 1));
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY['x', 'y', 'z'], ARRAY['abc', 'bcd', 'cde']), 10, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(3)), asList("cde", "bcd", "abc"));
    }

    @Test
    public void testNegativeN()
    {
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), -1, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList());
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[4, 5, 6], ARRAY[1, 2, 3]), -2, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(INTEGER), asList());
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[1, 2, 3], ARRAY['x', 'y', 'z']), 0, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1)))", new ArrayType(createVarcharType(1)), asList());
    }

    @Test
    public void testEmpty()
    {
        assertFunction("MAP_TOP_N_VALUES(MAP(ARRAY[], ARRAY[]), 1, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(UNKNOWN), asList());
    }

    @Test
    public void testNull()
    {
        assertFunction("MAP_TOP_N_VALUES(NULL, 1, (x, y) -> IF(x < y, 1, IF(x = y, 0, -1)))", new ArrayType(UNKNOWN), null);
    }
}
