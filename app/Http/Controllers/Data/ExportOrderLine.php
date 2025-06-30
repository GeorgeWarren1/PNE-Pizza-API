<?php

namespace App\Http\Controllers\Data;

use App\Models\OrderLine;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use App\Http\Controllers\Controller;

class ExportOrderLine extends Controller{
    public function exportcsv(Request $request, $startDateParam = null, $endDateParam = null, $franchiseStoreParam = null){
        // Get parameters from either URL segments or query parameters
        $startDate = $startDateParam ?? $request->query('start_date');
        $endDate = $endDateParam ?? $request->query('end_date');

        // Handle franchise store as a comma-separated list
        $franchiseStores = [];

        // First check if it was passed as a route parameter
        if (!empty($franchiseStoreParam)) {
            $franchiseStores = array_map('trim', explode(',', $franchiseStoreParam));
        } else {
            // Try franchise_store parameter from query
            $franchiseStoreString = $request->query('franchise_store');
            if (!empty($franchiseStoreString)) {
                // Check if it's a comma-separated string
                if (strpos($franchiseStoreString, ',') !== false) {
                    $franchiseStores = array_map('trim', explode(',', $franchiseStoreString));
                } else {
                    $franchiseStores = [$franchiseStoreString];
                }
            }
        }
        // Filter out empty values
        $franchiseStores = array_filter($franchiseStores, function($value) {
            return !empty($value) && $value !== 'null' && $value !== 'undefined';
        });

        // Build the query with filtering conditions
        $query = OrderLine::query();

        // Filter by business_date between startDate and endDate if both are provided
        if ($startDate && $endDate) {
            $query->whereBetween('business_date', [$startDate, $endDate]);
        }

        // Filter by franchise_store if provided
        if (!empty($franchiseStores)) {
            $query->whereIn('franchise_store', $franchiseStores);
        }


        try {
            // Retrieve the filtered data
            $data = $query->get();

            $recordCount = $data->count();
            Log::info('Finance data retrieved successfully', [
                'record_count' => $recordCount,
                'date_range' => $startDate && $endDate ? "$startDate to $endDate" : 'all dates',
                'franchise_stores' => !empty($franchiseStores) ? implode(', ', $franchiseStores) : 'all stores'
            ]);

            // Define the columns to export based on FinanceData model
            $columns = [
                'franchise_store',
                'business_date',
                'date_time_placed',
                'date_time_fulfilled',
                'net_amount',
                'quantity',

                'royalty_item',
                'taxable_item',
                'order_id',
                'item_id',
                'menu_item_name',
                'menu_item_account',
                'bundle_name',
                'employee',
                'override_approval_employee',
                'order_placed_method',
                'order_fulfilled_method',
                'modified_order_amount',
                'modification_reason',
                'payment_methods',
                'refunded',
                'tax_included_amount',
            ];

            // Define a callback that writes CSV rows directly to the output stream
            $callback = function() use ($data, $columns) {
                $file = fopen('php://output', 'w');

                // Write the header row
                fputcsv($file, $columns);

                // Write each record as a CSV row
                foreach ($data as $item) {
                    $row = [];
                    foreach ($columns as $col) {
                        $row[] = $item->{$col};
                    }
                    fputcsv($file, $row);
                }
                fclose($file);
            };

            // Generate a filename with filter information
            $filename = 'Order_Line';
            if ($startDate && $endDate) {
                $filename .= $startDate . '_to_' . $endDate;
            } else {
                $filename .= 'all_dates';
            }
            if (!empty($franchiseStores)) {
                $filename .= '_stores_' . count($franchiseStores);
            }
            $filename .= '.csv';

            Log::info('Finance Data CSV export completed', [
                'filename' => $filename,
                'record_count' => $recordCount
            ]);

            // Return a streaming download response using Laravel's streamDownload method
            return response()->streamDownload($callback, $filename, [
                'Content-Type' => 'text/csv',
            ]);
        } catch (\Exception $e) {
            Log::error('Error exporting Finance data', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);

            return response()->json([
                'success' => false,
                'message' => 'Failed to export Finance data: ' . $e->getMessage()
            ], 500);
        }
    }
}
