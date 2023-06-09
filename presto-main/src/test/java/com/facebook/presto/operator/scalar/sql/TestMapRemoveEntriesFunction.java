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
package com.facebook.presto.operator.scalar.sql;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.util.StructuralTestUtil.mapType;

public class TestMapRemoveEntriesFunction
        extends AbstractTestFunctions
{
    @Test
    public void testRetainedSizeBounded()
    {
        assertCachedInstanceHasBoundedRetainedSize("MAP_REMOVE_ENTRIES(MAP(ARRAY ['a', 'b', 'c', 'd'], ARRAY [1, 2, NULL, 4]), (k, v) -> v IS NOT NULL)");
    }

    @Test
    public void testEmpty()
    {
        assertFunction("MAP_REMOVE_ENTRIES(MAP(ARRAY[], ARRAY[]), (k, v) -> true)", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("MAP_REMOVE_ENTRIES(MAP(ARRAY[], ARRAY[]), (k, v) -> false)", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("MAP_REMOVE_ENTRIES(MAP(ARRAY[], ARRAY[]), (k, v) -> CAST (NULL AS BOOLEAN))", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("MAP_REMOVE_ENTRIES(CAST (MAP(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR)), (k, v) -> true)", mapType(BIGINT, VARCHAR), ImmutableMap.of());
    }

    @Test
    public void testNull()
    {
        Map<Integer, Void> oneToNullMap = new HashMap<>();
        oneToNullMap.put(1, null);
        assertFunction("MAP_REMOVE_ENTRIES(MAP(ARRAY[1], ARRAY [NULL]), (k, v) -> v IS NULL)", mapType(UNKNOWN, UNKNOWN), ImmutableMap.of());
        assertFunction("MAP_REMOVE_ENTRIES(MAP(ARRAY[1], ARRAY [NULL]), (k, v) -> v IS NOT NULL)", mapType(INTEGER, UNKNOWN), oneToNullMap);
        assertFunction("MAP_REMOVE_ENTRIES(MAP(ARRAY[1], ARRAY [CAST (NULL AS INTEGER)]), (k, v) -> v IS NOT NULL)", mapType(INTEGER, INTEGER), oneToNullMap);
        Map<Integer, Void> sequenceToNullMap = new HashMap<>();
        sequenceToNullMap.put(1, null);
        sequenceToNullMap.put(2, null);
        sequenceToNullMap.put(3, null);
        assertFunction("MAP_REMOVE_ENTRIES(MAP(ARRAY[1, 2, 3], ARRAY [NULL, NULL, NULL]), (k, v) -> v IS NULL)", mapType(INTEGER, UNKNOWN), ImmutableMap.of());
        assertFunction("MAP_REMOVE_ENTRIES(MAP(ARRAY[1, 2, 3], ARRAY [NULL, NULL, NULL]), (k, v) -> v IS NOT NULL)", mapType(INTEGER, UNKNOWN), sequenceToNullMap);
    }

    @Test
    public void testBasic()
    {
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [5, 6, 7, 8], ARRAY [5, 6, 6, 5]), (k, y) -> k <= 6 OR y = 5)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(7, 6));
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [5 + RANDOM(1), 6, 7, 8], ARRAY [5, 6, 6, 5]), (k, y) -> k > 6 OR y = 5)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(6, 6));

        HashMap hashMap = new HashMap<>();
        hashMap.put("c", null);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY ['a', 'b', 'c', 'd'], ARRAY [1, 2, NULL, 4]), (k, v) -> v IS NOT NULL)",
                mapType(createVarcharType(1), INTEGER),
                hashMap);
        hashMap.put("b", false);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY ['a', 'b', 'c'], ARRAY [TRUE, FALSE, NULL]), (k, v) -> v)",
                mapType(createVarcharType(1), BOOLEAN),
                hashMap);
    }

    @Test
    public void testTypeCombinations()
    {
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [25, 26, 27], ARRAY [25, 26, 27]), (k, v) -> k = 25 OR v = 27)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(26, 26));
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [25, 26, 27], ARRAY [25.5E0, 26.5E0, 27.5E0]), (k, v) -> k != 25 OR v != 27.5E0)",
                mapType(INTEGER, DOUBLE),
                ImmutableMap.of());

        HashMap hashMap = new HashMap<>();
        hashMap.put(26, null);
        hashMap.put(27, true);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [25, 26, 27], ARRAY [false, null, true]), (k, v) -> k = 25 OR NOT v)",
                mapType(INTEGER, BOOLEAN),
                hashMap);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [25, 26, 27], ARRAY ['abc', 'def', 'xyz']), (k, v) -> k != 25 OR v = 'abc')",
                mapType(INTEGER, createVarcharType(3)),
                ImmutableMap.of());
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [25, 26, 27], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'c'], ARRAY ['a', 'b', 'c']]), (k, v) -> k = 25 OR cardinality(v) = 3)",
                mapType(INTEGER, new ArrayType(createVarcharType(1))),
                ImmutableMap.of(26, ImmutableList.of("a", "c")));

        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY [25, 26, 27]), (k, v) -> k = 25.5E0 OR v != 27)",
                mapType(DOUBLE, INTEGER),
                ImmutableMap.of(27.5, 27));
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY [25.5E0, 26.5E0, 27.5E0]), (k, v) -> k > 25.5E0 AND v >= 27.5E0)",
                mapType(DOUBLE, DOUBLE),
                ImmutableMap.of(25.5, 25.5, 26.5, 26.5));

        hashMap.clear();
        hashMap.put(26.5, null);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY [false, null, true]), (k, v) -> k = 25.5E0 OR v)",
                mapType(DOUBLE, BOOLEAN),
                hashMap);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY ['abc', 'def', 'xyz']), (k, v) -> k = 25.5E0 OR v = 'xyz')",
                mapType(DOUBLE, createVarcharType(3)),
                ImmutableMap.of(26.5, "def"));
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'c'], ARRAY ['a', 'b', 'c']]), (k, v) -> k = 25.5E0 OR cardinality(v) < 3)",
                mapType(DOUBLE, new ArrayType(createVarcharType(1))),
                ImmutableMap.of(27.5, ImmutableList.of("a", "b", "c")));

        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [true, false], ARRAY [25, 26]), (k, v) -> k AND v = 25)",
                mapType(BOOLEAN, INTEGER),
                ImmutableMap.of(false, 26));
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [false, true], ARRAY [25.5E0, 26.5E0]), (k, v) -> k OR v > 100)",
                mapType(BOOLEAN, DOUBLE),
                ImmutableMap.of(false, 25.5));

        Map<Boolean, Boolean> falseToNullMap = new HashMap<>();
        falseToNullMap.put(false, null);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [true, false], ARRAY [false, null]), (k, v) -> k OR NOT v)",
                mapType(BOOLEAN, BOOLEAN),
                falseToNullMap);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [false, true], ARRAY ['abc', 'def']), (k, v) -> NOT k AND v = 'abc')",
                mapType(BOOLEAN, createVarcharType(3)),
                ImmutableMap.of(true, "def"));
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [true, false], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'b', 'c']]), (k, v) -> NOT k AND cardinality(v) = 2)",
                mapType(BOOLEAN, new ArrayType(createVarcharType(1))),
                ImmutableMap.of(true, ImmutableList.of("a", "b"), false, ImmutableList.of("a", "b", "c")));

        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY ['s0', 's1', 's2'], ARRAY [25, 26, 27]), (k, v) -> k = 's0' OR v = 27)",
                mapType(createVarcharType(2), INTEGER),
                ImmutableMap.of("s1", 26));
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY ['s0', 's1', 's2'], ARRAY [25.5E0, 26.5E0, 27.5E0]), (k, v) -> k = 's0' OR v = 27.5E0)",
                mapType(createVarcharType(2), DOUBLE),
                ImmutableMap.of("s1", 26.5));

        hashMap.clear();
        hashMap.put("s1", null);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY ['s0', 's1', 's2'], ARRAY [false, null, true]), (k, v) -> k = 's0' OR v)",
                mapType(createVarcharType(2), BOOLEAN),
                hashMap);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY ['s0', 's1', 's2'], ARRAY ['abc', 'def', 'xyz']), (k, v) -> k = 's0' AND v = 'xyz')",
                mapType(createVarcharType(2), createVarcharType(3)),
                ImmutableMap.of("s0", "abc", "s1", "def", "s2", "xyz"));
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY ['s0', 's1', 's2'], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'c'], ARRAY ['a', 'b', 'c']]), (k, v) -> k = 's0' OR cardinality(v) = 3)",
                mapType(createVarcharType(2), new ArrayType(createVarcharType(1))),
                ImmutableMap.of("s1", ImmutableList.of("a", "c")));

        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [ARRAY [1, 2], ARRAY [3, 4], ARRAY []], ARRAY [25, 26, 27]), (k, v) -> k = ARRAY [1, 2] AND v = 27)",
                mapType(new ArrayType(INTEGER), INTEGER),
                ImmutableMap.of(ImmutableList.of(1, 2), 25, ImmutableList.of(3, 4), 26, ImmutableList.of(), 27));
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [ARRAY [1, 2], ARRAY [3, 4], ARRAY []], ARRAY [25.5E0, 26.5E0, 27.5E0]), (k, v) -> k = ARRAY [1, 2] OR v = 25.5E0)",
                mapType(new ArrayType(INTEGER), DOUBLE),
                ImmutableMap.of(ImmutableList.of(3, 4), 26.5, ImmutableList.of(), 27.5));

        hashMap.clear();
        hashMap.put(ImmutableList.of(1, 2), false);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [ARRAY [1, 2], ARRAY [3, 4], ARRAY []], ARRAY [false, null, true]), (k, v) -> k != ARRAY [1, 2] OR v)",
                mapType(new ArrayType(INTEGER), BOOLEAN),
                hashMap);
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [ARRAY [1, 2], ARRAY [3, 4], ARRAY []], ARRAY ['abc', 'def', 'xyz']), (k, v) -> k = ARRAY [1, 2] OR v = 'xyz')",
                mapType(new ArrayType(INTEGER), createVarcharType(3)),
                ImmutableMap.of(ImmutableList.of(3, 4), "def"));
        assertFunction(
                "MAP_REMOVE_ENTRIES(MAP(ARRAY [ARRAY [1, 2], ARRAY [3, 4], ARRAY []], ARRAY [ARRAY ['a', 'b'], ARRAY ['a', 'b', 'c'], ARRAY ['a', 'c']]), (k, v) -> cardinality(k) = 0 OR cardinality(v) = 3)",
                mapType(new ArrayType(INTEGER), new ArrayType(createVarcharType(1))),
                ImmutableMap.of(ImmutableList.of(1, 2), ImmutableList.of("a", "b"), ImmutableList.of(), ImmutableList.of("a", "c")));
    }
}
