package org.concurrent;

import org.junit.Test;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class MainTest {

    private List<int[]> sampleData;

    @Before
    public void setUp() {
        sampleData = Arrays.asList(
                new int[]{5, 10, 15, 20, 25, 30},
                new int[]{3, 6, 9, 12, 15, 18},
                new int[]{4, 8, 12, 16, 20, 24}
        );
    }

    @Test
    public void testCalculateTotalUnitsSold() throws InterruptedException, ExecutionException {
        int[] result = Main.calculateTotalUnitsSold(Main.createExecutor(), sampleData);
        assertEquals(12, result[0]); // 5 + 3 + 4
        assertEquals(24, result[1]); // 10 + 6 + 8
        assertEquals(36, result[2]); // 15 + 9 + 12
        assertEquals(48, result[3]); // 20 + 12 + 16
        assertEquals(60, result[4]); // 25 + 15 + 20
        assertEquals(72, result[5]); // 30 + 18 + 24
    }

    @Test
    public void testCalculateTotalDailyProfit() throws InterruptedException, ExecutionException {
        double result = Main.calculateTotalDailyProfit(Main.createExecutor(), sampleData);
        double expected = (5 * 1.10 + 10 * 1.50 + 15 * 2.10 + 20 * 1.60 + 25 * 1.80 + 30 * 3.90) +
                (3 * 1.10 + 6 * 1.50 + 9 * 2.10 + 12 * 1.60 + 15 * 1.80 + 18 * 3.90) +
                (4 * 1.10 + 8 * 1.50 + 12 * 2.10 + 16 * 1.60 + 20 * 1.80 + 24 * 3.90);
        assertEquals(expected, result, 0.01);
    }

    @Test
    public void testCalculateBranchWithLowestProfit() throws InterruptedException, ExecutionException {
        Main.BranchProfit result = Main.calculateBranchWithLowestProfit(Main.createExecutor(), sampleData);
        double branch1Profit = 5 * 1.10 + 10 * 1.50 + 15 * 2.10 + 20 * 1.60 + 25 * 1.80 + 30 * 3.90;
        double branch2Profit = 3 * 1.10 + 6 * 1.50 + 9 * 2.10 + 12 * 1.60 + 15 * 1.80 + 18 * 3.90;
        double branch3Profit = 4 * 1.10 + 8 * 1.50 + 12 * 2.10 + 16 * 1.60 + 20 * 1.80 + 24 * 3.90;
        double lowestProfit = Math.min(branch1Profit, Math.min(branch2Profit, branch3Profit));
        assertEquals(String.format("%06d", lowestProfit == branch1Profit ? 1 : (lowestProfit == branch2Profit ? 2 : 3)), result.branchId);
        assertEquals(lowestProfit, result.profit, 0.01);
    }
}
