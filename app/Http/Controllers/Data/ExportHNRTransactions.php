<?php

namespace App\Http\Controllers\Data;


use Illuminate\Http\Request;
use Illuminate\Support\Facades\Log;
use App\Http\Controllers\Controller;

use App\Models\HourHNRTransaction;
use App\Models\StoreHNRTransaction;

class ExportHNRTransactions extends Controller
{

    public function Csv_HourHNRTransactions(Request $request, $startDateParam = null, $endDateParam = null,$hoursParm= null, $franchiseStoreParam = null)
    {
        Log::info('Hourly Sales export requested', [
            'ip' => $request->ip(),
            'user_agent' => $request->header('User-Agent')
        ]);

        // Get parameters from either URL segments or query parameters
        $startDate = $startDateParam ?? $request->query('start_date');
        $endDate = $endDateParam ?? $request->query('end_date');

        // Handle franchise store as a comma-separated list
        $franchiseStores = [];
        $hours = [];

        // hours
        if ($hoursParm !== null) {
        $hours = array_map('trim', explode(',', $hoursParm));
        } else {
            $hoursString = $request->query('hours');
            if ($hoursString !== null) {
                if (strpos($hoursString, ',') !== false) {
                    $hours = array_map('trim', explode(',', $hoursString));
                } else {
                    $hours = [$hoursString];
                }
            }
        }

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

        $hours = array_filter($hours, function($value) {
        return is_numeric($value);
        });
        $hours = array_map('intval', $hours);

        Log::debug('Export parameters', [
            'start_date' => $startDate,
            'end_date' => $endDate,
            'franchise_stores' => $franchiseStores,
            'raw_query' => $request->getQueryString()
        ]);

        // Build the query with filtering conditions
        $query = HourHNRTransaction::query();

        // Filter by business_date between startDate and endDate if both are provided
        if ($startDate && $endDate) {
            $query->whereBetween('business_date', [$startDate, $endDate]);
        }

        // Filter by franchise_store if provided
        if (!empty($franchiseStores)) {
            $query->whereIn('franchise_store', $franchiseStores);
        }

        Log::debug('Filtering by hours:', ['hours' => $hours]);

        if (!is_null($hours) && $hours !== []) {
        $query = $this->safeWhereIn($query, 'hour', $hours);
        }

        try {
            // Retrieve the filtered data
            $data = $query->get();

            $recordCount = $data->count();
            Log::info('Hourly Sales data retrieved successfully', [
                'record_count' => $recordCount,
                'date_range' => $startDate && $endDate ? "$startDate to $endDate" : 'all dates',
                'franchise_stores' => !empty($franchiseStores) ? implode(', ', $franchiseStores) : 'all stores'
            ]);

            // Define the columns to export based on HourlySales model
            $columns = [
                'id',
                'franchise_store',
                'business_date',
                'hour',
                'transactions',
                'promise_broken_transactions',
                'promise_broken_percentage',
                'created_at',
                'updated_at'
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
            $filename = 'hourly_sales_';
            if ($startDate && $endDate) {
                $filename .= $startDate . '_to_' . $endDate;
            } else {
                $filename .= 'all_dates';
            }
            if (!empty($franchiseStores)) {
                $filename .= '_stores_' . count($franchiseStores);
            }
            $filename .= '.csv';

            Log::info('Hourly Sales CSV export completed', [
                'filename' => $filename,
                'record_count' => $recordCount
            ]);

            // Return a streaming download response using Laravel's streamDownload method
            return response()->streamDownload($callback, $filename, [
                'Content-Type' => 'text/csv',
            ]);
        } catch (\Exception $e) {
            Log::error('Error exporting Hourly Sales data', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);

            return response()->json([
                'success' => false,
                'message' => 'Failed to export Hourly Sales data: ' . $e->getMessage()
            ], 500);
        }
    }


    public function Json_HourHNRTransactions(Request $request, $startDateParam = null, $endDateParam = null, $hoursParm = null, $franchiseStoreParam = null)
    {
        Log::info('Hourly Sales JSON export requested', [
            'ip'         => $request->ip(),
            'user_agent' => $request->header('User-Agent'),
        ]);

        // 1) Dates
        $startDate = $startDateParam ?? $request->query('start_date');
        $endDate   = $endDateParam   ?? $request->query('end_date');

        // 2) Franchise stores (array of strings)
        $franchiseStores = [];
        if ($franchiseStoreParam) {
            $franchiseStores = array_map('trim', explode(',', $franchiseStoreParam));
        } elseif ($request->filled('franchise_store')) {
            $franchiseStores = array_map('trim', explode(',', $request->query('franchise_store')));
        }
        $franchiseStores = array_filter($franchiseStores, function($v) {
            return $v !== '' && strtolower($v) !== 'null' && strtolower($v) !== 'undefined';
        });

        // 3) Hours (array of strings)
        $hours = [];
        if ($hoursParm !== null) {
            $hours = array_map('trim', explode(',', $hoursParm));
        } elseif ($request->filled('hours')) {
            $hours = array_map('trim', explode(',', $request->query('hours')));
        }
        $hours = array_filter($hours, function($v) {
            return $v !== '' && strtolower($v) !== 'null' && strtolower($v) !== 'undefined';
        });

        Log::debug('JSON export parameters', [
            'start_date'       => $startDate,
            'end_date'         => $endDate,
            'franchise_stores' => $franchiseStores,
            'hours'            => $hours,
            'raw_query'        => $request->getQueryString(),
        ]);

        // 4) Build query
        $query = HourHNRTransaction::query();

        if ($startDate && $endDate) {
            $query->whereBetween('business_date', [$startDate, $endDate]);
        }
        if (!empty($franchiseStores)) {
            $query->whereIn('franchise_store', $franchiseStores);
        }
        if (!empty($hours)) {
            $query = $this->safeWhereIn($query, 'hour', $hours);
        }

        try {
            $data = $query->get();
            $count = $data->count();

            Log::info('Hourly Sales JSON data retrieved successfully', [
                'record_count'     => $count,
                'date_range'       => $startDate && $endDate ? "$startDate to $endDate" : 'all dates',
                'franchise_stores' => $franchiseStores ?: 'all stores',
                'hours'            => $hours ?: 'all hours',
            ]);

            return response()->json([
                'success'       => true,
                'record_count'  => $count,
                'data'          => $data,
            ], 200);

        } catch (\Exception $e) {
            Log::error('Error exporting Hourly Sales JSON data', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);

            return response()->json([
                'success' => false,
                'message' => 'Failed to export Hourly Sales data: ' . $e->getMessage(),
            ], 500);
        }
    }


    public function Csv_StoreHNRTransactions(Request $request, $startDateParam = null, $endDateParam = null, $franchiseStoreParam = null){
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
        $query = StoreHNRTransaction::query();

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
                'item_id',
                'item_name',
                'transactions',
                'promise_met_transactions',
                'promise_met_percentage',
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
    public function Json_StoreHNRTransactions(Request $request, $startDateParam = null, $endDateParam = null, $franchiseStoreParam = null)
    {
        Log::info('Store HNR JSON export requested', [
            'ip'         => $request->ip(),
            'user_agent' => $request->header('User-Agent'),
        ]);

        // 1) Dates from route or query
        $startDate = $startDateParam ?? $request->query('start_date');
        $endDate   = $endDateParam   ?? $request->query('end_date');

        // 2) franchise_store as array of strings
        $franchiseStores = [];
        if (!empty($franchiseStoreParam)) {
            $franchiseStores = array_map('trim', explode(',', $franchiseStoreParam));
        } elseif ($request->filled('franchise_store')) {
            $franchiseStores = array_map('trim', explode(',', $request->query('franchise_store')));
        }
        $franchiseStores = array_filter($franchiseStores, function($v) {
            return $v !== '' && strtolower($v) !== 'null' && strtolower($v) !== 'undefined';
        });

        Log::debug('JSON export parameters', [
            'start_date'       => $startDate,
            'end_date'         => $endDate,
            'franchise_stores' => $franchiseStores,
            'raw_query'        => $request->getQueryString(),
        ]);

        // 3) Build Eloquent query
        $query = StoreHNRTransaction::query();

        if ($startDate && $endDate) {
            $query->whereBetween('business_date', [$startDate, $endDate]);
        }
        if (!empty($franchiseStores)) {
            $query->whereIn('franchise_store', $franchiseStores);
        }

        try {
            $data = $query->get();
            $count = $data->count();

            Log::info('Store HNR data retrieved successfully', [
                'record_count'     => $count,
                'date_range'       => $startDate && $endDate ? "$startDate to $endDate" : 'all dates',
                'franchise_stores' => $franchiseStores ?: 'all stores',
            ]);

            return response()->json([
                'success'      => true,
                'record_count' => $count,
                'data'         => $data,
            ], 200);
        }
        catch (\Exception $e) {
            Log::error('Error exporting Store HNR JSON data', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);

            return response()->json([
                'success' => false,
                'message' => 'Failed to export Store HNR data: ' . $e->getMessage(),
            ], 500);
        }
    }


    protected function safeWhereIn($query, $column, $values)
    {
        if (!is_array($values) || count($values) === 0) {
            return $query;
        }

        $placeholders = implode(',', array_fill(0, count($values), '?'));
        return $query->whereRaw("$column IN ($placeholders)", $values);
    }
}
