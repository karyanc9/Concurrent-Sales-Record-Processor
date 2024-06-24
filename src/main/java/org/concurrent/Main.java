package org.concurrent;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Main {

    private static final String FILE_PATH = "sales_records.xlsx"; // Path to the Excel file containing sales data
    private static final int NUM_THREADS = 8; // Number of threads to use for concurrent processing

    // Profit margins for each product
    private static final double[] PRODUCT_PROFITS = {1.10, 1.50, 2.10, 1.60, 1.80, 3.90};
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##"); // Format for displaying profits

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        long startTime, endTime;

        // Creating a thread pool with a fixed number of threads
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        // Read sales data from Excel file
        List<int[]> salesData = readSalesData(FILE_PATH);

        // Calculate total units sold for each product
        startTime = System.currentTimeMillis();
        int[] totalUnitsSold = calculateTotalUnitsSold(executor, salesData);
        endTime = System.currentTimeMillis();
        System.out.println("Total units sold: ");
        printUnitsSold(totalUnitsSold);
        // Uncomment the line below to display time taken for task process
        // System.out.println("Time taken for identifying total units sold: " + (endTime - startTime) + "ms");

        // Calculate total daily profits
        startTime = System.currentTimeMillis();
        double totalDailyProfit = calculateTotalDailyProfit(executor, salesData);
        endTime = System.currentTimeMillis();
        System.out.println("\nTotal daily profits: " + DECIMAL_FORMAT.format(totalDailyProfit));
        // Uncomment the line below to display time taken for task process
        // System.out.println("Time taken for identifying total daily profits: " + (endTime - startTime) + "ms");

        // Identify branch with lowest daily profit
        startTime = System.currentTimeMillis();
        BranchProfit branchWithLowestProfit = calculateBranchWithLowestProfit(executor, salesData);
        endTime = System.currentTimeMillis();
        System.out.println("\nBranch with lowest profit: " + branchWithLowestProfit.branchId);
        System.out.println("Profit of branch with lowest profit: " + DECIMAL_FORMAT.format(branchWithLowestProfit.profit));
        // Uncomment the line below to display time taken for task process
        // System.out.println("Time taken for identifying branch with lowest profit: " + (endTime - startTime) + "ms");

        // Shutting down the executor service
        executor.shutdown();
    }

    /**
     * Reads sales data from the specified Excel file.
     *
     * @param filePath Path to the Excel file containing sales data
     * @return List of int arrays representing sales data records
     * @throws IOException If an error occurs while reading the file
     */
    private static List<int[]> readSalesData(String filePath) throws IOException {
        List<int[]> salesData = new ArrayList<>();
        try (FileInputStream file = new FileInputStream(filePath);
             Workbook workbook = new XSSFWorkbook(file)) {
            Sheet sheet = workbook.getSheetAt(0);
            // Iterate through rows to read sales data
            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                int[] record = new int[6];
                // Iterate through columns (products A to F)
                for (int j = 1; j <= 6; j++) {
                    record[j - 1] = (int) row.getCell(j).getNumericCellValue();
                }
                salesData.add(record); // Add each record to sales data list
            }
        }
        return salesData;
    }

    /**
     * Calculates total units sold for each product using multiple threads.
     *
     * @param executor  ExecutorService managing the thread pool
     * @param salesData List of sales data records
     * @return Array representing total units sold for each product
     * @throws InterruptedException If any thread is interrupted while waiting
     * @throws ExecutionException   If an error occurs during execution of any thread
     */
    private static int[] calculateTotalUnitsSold(ExecutorService executor, List<int[]> salesData)
            throws InterruptedException, ExecutionException {
        int[] totalUnitsSold = new int[6];
        List<Future<int[]>> futuresUnits = new ArrayList<>();

        // Submit tasks to calculate units sold for each thread
        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadId = i;
            futuresUnits.add(executor.submit(() -> calculateUnitsSold(salesData, threadId)));
        }

        // Aggregate results from all threads
        for (Future<int[]> future : futuresUnits) {
            int[] units = future.get();
            for (int i = 0; i < totalUnitsSold.length; i++) {
                totalUnitsSold[i] += units[i];
            }
        }
        return totalUnitsSold;
    }

    /**
     * Calculates total daily profits using multiple threads.
     *
     * @param executor  ExecutorService managing the thread pool
     * @param salesData List of sales data records
     * @return Total daily profit
     * @throws InterruptedException If any thread is interrupted while waiting
     * @throws ExecutionException   If an error occurs during execution of any thread
     */
    private static double calculateTotalDailyProfit(ExecutorService executor, List<int[]> salesData)
            throws InterruptedException, ExecutionException {
        double totalProfit = 0;
        List<Future<Double>> futuresProfits = new ArrayList<>();

        // Submit tasks to calculate total profits for each thread
        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadId = i;
            futuresProfits.add(executor.submit(() -> calculateTotalProfits(salesData, threadId)));
        }

        // Aggregate results from all threads
        for (Future<Double> future : futuresProfits) {
            totalProfit += future.get();
        }
        return totalProfit;
    }

    /**
     * Calculates the branch with the lowest daily profit using multiple threads.
     *
     * @param executor  ExecutorService managing the thread pool
     * @param salesData List of sales data records
     * @return BranchProfit object representing branch with the lowest profit
     * @throws InterruptedException If any thread is interrupted while waiting
     * @throws ExecutionException   If an error occurs during execution of any thread
     */
    private static BranchProfit calculateBranchWithLowestProfit(ExecutorService executor, List<int[]> salesData)
            throws InterruptedException, ExecutionException {
        double minProfit = Double.MAX_VALUE;
        String branchId = "";
        BranchProfit result = null;
        List<Future<BranchProfit>> futuresBranchProfits = new ArrayList<>();

        // Submit tasks to find branch with lowest profit for each thread
        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadId = i;
            futuresBranchProfits.add(executor.submit(() -> calculateBranchWithLowestProfitTask(salesData, threadId)));
        }

        // Aggregate results from all threads
        for (Future<BranchProfit> future : futuresBranchProfits) {
            BranchProfit branchProfit = future.get();
            // Determine the branch with the lowest profit
            if (branchProfit.profit < minProfit) {
                minProfit = branchProfit.profit;
                branchId = branchProfit.branchId;
                result = branchProfit;
            }
        }
        return result;
    }

    /**
     * Calculates total units sold for each product within a specified thread's chunk of data.
     *
     * @param salesData List of sales data records
     * @param threadId  Identifier of the thread
     * @return Array representing total units sold for each product in the specified chunk
     */
    private static int[] calculateUnitsSold(List<int[]> salesData, int threadId) {
        int[] totalUnitsSold = new int[6];
        int chunkSize = (int) Math.ceil(salesData.size() / (double) NUM_THREADS); // Calculate chunk size for each thread
        int start = threadId * chunkSize; // Start index for the current thread
        int end = Math.min(start + chunkSize, salesData.size()); // End index for the current thread

        // Calculate total units sold for each product in the chunk of data
        for (int i = start; i < end; i++) {
            int[] record = salesData.get(i);
            for (int j = 0; j < totalUnitsSold.length; j++) {
                totalUnitsSold[j] += record[j];
            }
        }
        return totalUnitsSold;
    }

    /**
     * Calculates total profits for all products within a specified thread's chunk of data.
     *
     * @param salesData List of sales data records
     * @param threadId  Identifier of the thread
     * @return Total profits for the specified chunk
     */
    private static double calculateTotalProfits(List<int[]> salesData, int threadId) {
        double totalProfit = 0;
        int chunkSize = (int) Math.ceil(salesData.size() / (double) NUM_THREADS); // Calculate chunk size for each thread
        int start = threadId * chunkSize; // Start index for the current thread
        int end = Math.min(start + chunkSize, salesData.size()); // End index for the current thread

        // Calculate total profits for all products in the chunk of data
        for (int i = start; i < end; i++) {
            int[] record = salesData.get(i);
            for (int j = 0; j < record.length; j++) {
                totalProfit += record[j] * PRODUCT_PROFITS[j];
            }
        }
        return totalProfit;
    }

    /**
     * Identifies the branch with the lowest profit within a specified thread's chunk of data.
     *
     * @param salesData List of sales data records
     * @param threadId  Identifier of the thread
     * @return BranchProfit object representing branch with the lowest profit in the specified chunk
     */
    private static BranchProfit calculateBranchWithLowestProfitTask(List<int[]> salesData, int threadId) {
        double minProfit = Double.MAX_VALUE;
        String branchId = "";
        int chunkSize = (int) Math.ceil(salesData.size() / (double) NUM_THREADS); // Calculate chunk size for each thread
        int start = threadId * chunkSize; // Start index for the current thread
        int end = Math.min(start + chunkSize, salesData.size()); // End index for the current thread

        // Find the branch with the lowest profit in the chunk of data
        for (int i = start; i < end; i++) {
            double profit = 0;
            int[] record = salesData.get(i);
            for (int j = 0; j < record.length; j++) {
                profit += record[j] * PRODUCT_PROFITS[j];
            }
            if (profit < minProfit) {
                minProfit = profit;
                branchId = String.format("%06d", i + 1);
            }
        }
        return new BranchProfit(branchId, minProfit);
    }

    /**
     * Utility method to print total units sold for each product.
     *
     * @param totalUnitsSold Array representing total units sold for each product
     */
    private static void printUnitsSold(int[] totalUnitsSold) {
        System.out.println("+-----------+-----------+-----------+-----------+-----------+-----------+");
        System.out.println("| Product A | Product B | Product C | Product D | Product E | Product F |");
        System.out.println("+-----------+-----------+-----------+-----------+-----------+-----------+");
        System.out.printf("| %9d | %9d | %9d | %9d | %9d | %9d |\n",
                totalUnitsSold[0], totalUnitsSold[1], totalUnitsSold[2],
                totalUnitsSold[3], totalUnitsSold[4], totalUnitsSold[5]);
        System.out.println("+-----------+-----------+-----------+-----------+-----------+-----------+");
    }

    /**
     * Class to represent branch profit data.
     */
    static class BranchProfit {
        String branchId;
        double profit;

        BranchProfit(String branchId, double profit) {
            this.branchId = branchId;
            this.profit = profit;
        }
    }
}
