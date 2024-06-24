package org.concurrent;

import org.junit.Test;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BoundaryTest {

    private List<int[]> emptyData;
    private List<int[]> singleRecordData;

    @Before
    public void setUp() {
        emptyData = Collections.emptyList();
        singleRecordData = Collections.singletonList(new int[]{10, 20, 30, 40, 50, 60});
    }

    @Test
    public void testEmptyDataset() {
        try {
            Main.calculateTotalUnitsSold(Main.createExecutor(), emptyData);
            fail("Expected an exception for empty dataset");
        } catch (IllegalArgumentException | InterruptedException | ExecutionException e) {
            // Expected exception
        }

        try {
            Main.calculateTotalDailyProfit(Main.createExecutor(), emptyData);
            fail("Expected an exception for empty dataset");
        } catch (IllegalArgumentException | InterruptedException | ExecutionException e) {
            // Expected exception
        }

        try {
            Main.calculateBranchWithLowestProfit(Main.createExecutor(), emptyData);
            fail("Expected an exception for empty dataset");
        } catch (IllegalArgumentException | InterruptedException | ExecutionException e) {
            // Expected exception
        }
    }

    @Test
    public void testSingleRecordDataset() throws InterruptedException, ExecutionException {
        int[] totalUnitsSold = Main.calculateTotalUnitsSold(Main.createExecutor(), singleRecordData);
        assertEquals(10, totalUnitsSold[0]);
        assertEquals(20, totalUnitsSold[1]);
        assertEquals(30, totalUnitsSold[2]);
        assertEquals(40, totalUnitsSold[3]);
        assertEquals(50, totalUnitsSold[4]);
        assertEquals(60, totalUnitsSold[5]);

        double totalDailyProfit = Main.calculateTotalDailyProfit(Main.createExecutor(), singleRecordData);
        double expectedProfit = 10 * 1.10 + 20 * 1.50 + 30 * 2.10 + 40 * 1.60 + 50 * 1.80 + 60 * 3.90;
        assertEquals(expectedProfit, totalDailyProfit, 0.01);

        Main.BranchProfit branchWithLowestProfit = Main.calculateBranchWithLowestProfit(Main.createExecutor(), singleRecordData);
        assertEquals("000001", branchWithLowestProfit.branchId);
        assertEquals(expectedProfit, branchWithLowestProfit.profit, 0.01);
    }
}
