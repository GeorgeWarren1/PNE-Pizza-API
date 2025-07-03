<?php

namespace App\Services;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use Illuminate\Support\Facades\Log;
use Carbon\Carbon;

use App\Models\CashManagement;
use App\Models\FinancialView;
use App\Models\SummaryItem;
use App\Models\SummarySale;
use App\Models\SummaryTransaction;
use App\Models\DetailOrder;
use App\Models\OrderLine;
use App\Models\Waste;
use App\Models\FinalSummary;
use App\Models\HourlySales;
use App\Models\FinanceData;
use App\Models\OnlineDiscountProgram;
use App\Models\DeliveryOrderSummary;
use App\Models\ThirdPartyMarketplaceOrder;
use App\Models\BreadBoostModel;
use App\Models\ChannelData;

class LCReportDataService
{
    public function importReportData($selectedDate)
    {
        $client = new Client();

        Log::info('Starting report data import process for date: ' . $selectedDate);

        // Step 1: Generate Bearer Token
        try {
            // Log::info('Attempting to generate Bearer Token');

            $response = $client->post(config('services.lcegateway.portal_server') . '/Token', [
                'form_params' => [
                    'grant_type' => 'password',
                    'UserName' => config('services.lcegateway.username'),
                    'Password' => config('services.lcegateway.password'),
                ],
                'headers' => [
                    'Accept' => 'application/json,text/plain,*/*',
                    'Content-Type' => 'application/x-www-form-urlencoded',
                ],
            ]);

            $body = json_decode($response->getBody(), true);
            // Log::info('Bearer Token response body: ' . json_encode($body));

            $accessToken = $body['access_token'] ?? null;

            if (!$accessToken) {
                Log::error('Failed to obtain access token: access_token is missing in the response.');
                return false;
            }

            // Log::info('Successfully obtained access token.');

        } catch (RequestException $e) {
            Log::error('Error obtaining access token: ' . $e->getMessage());
            return false;
        }

        // Step 2: Download the Report
        try {
            //  Log::info('Preparing to download the report.');

            // Prepare variables
            $httpMethod = 'GET';
            $endpoint = '/GetReportBlobs';
            $userName = config('services.lcegateway.username');

            // Use the static storeId
            $storeId = '03795';

            //   Log::info('Using userName: ' . $userName);
            //   Log::info('Using static storeId: ' . $storeId);

            // Use the selected date
            $fileName = $storeId . '_' . $selectedDate . '.zip';
            //   Log::info('Constructed fileName: ' . $fileName);

            $queryParams = [
                'userName' => $userName,
                'fileName' => $fileName,
            ];

            $requestUrl = config('services.lcegateway.portal_server') . $endpoint . '?' . http_build_query($queryParams);
            //     Log::info('Constructed request URL: ' . $requestUrl);

            // Build the URL for the signature
            $encodedRequestUrl = $this->prepareRequestUrlForSignature($requestUrl);
            //       Log::info('Encoded request URL for signature: ' . $encodedRequestUrl);

            // Generate timestamp and nonce
            $requestTimeStamp = time();
            $nonce = $this->generateNonce();

            //   Log::info('Generated request timestamp: ' . $requestTimeStamp);
            //  Log::info('Generated nonce: ' . $nonce);

            // For GET requests, bodyHash is empty

            $bodyHash = '';

            // Prepare signature raw data
            $appId = config('services.lcegateway.hmac_user');
            $apiKey = config('services.lcegateway.hmac_key');
            $signatureRawData = $appId . $httpMethod . $encodedRequestUrl . $requestTimeStamp . $nonce . $bodyHash;

            //  Log::info('Signature raw data: ' . $signatureRawData);

            // Compute HMAC SHA256
            $key = base64_decode($apiKey);
            //    Log::info('Decoded API key from Base64.');
            $hash = hash_hmac('sha256', $signatureRawData, $key, true);
            //     Log::info('Computed HMAC SHA256 hash.');
            $hashInBase64 = base64_encode($hash);
            //      Log::info('Encoded hash in Base64: ' . $hashInBase64);

            // Prepare the authorization header
            $authHeader = 'amx ' . $appId . ':' . $hashInBase64 . ':' . $nonce . ':' . $requestTimeStamp;
            //    Log::info('Constructed HMacAuthorizationHeader: ' . $authHeader);

            // Make the GET request to download the report
            //      Log::info('Making GET request to download the report.');

            $response = $client->get($requestUrl, [
                'headers' => [
                    'HMacAuthorizationHeader' => $authHeader,
                    'Content-Type' => 'application/json',
                    'Authorization' => 'bearer ' . $accessToken,
                ],
                'stream' => true,
            ]);

            //    Log::info('Received response from report download request.');

            // Determine the content type
            $contentType = $response->getHeaderLine('Content-Type');
            //     Log::info('Response content type: ' . $contentType);

            // Read the response body as a string
            $bodyString = $response->getBody()->getContents();
            //    Log::info('Response body string: ' . $bodyString);

            // Decode the response body
            $decodedBodyOnce = json_decode($bodyString, true);
            //     Log::info('Decoded body after first json_decode: ' . json_encode($decodedBodyOnce));

            if (is_string($decodedBodyOnce)) {
                // Decode again
                $decodedBody = json_decode($decodedBodyOnce, true);
                // Log::info('Decoded body after second json_decode: ' . json_encode($decodedBody));
            } else {
                $decodedBody = $decodedBodyOnce;
            }

            $start = microtime(true);

            if (isset($decodedBody[0]['ReportBlobUri'])) {
                $downloadUrl = $decodedBody[0]['ReportBlobUri'];
                //   Log::info('Download URL: ' . $downloadUrl);

                $timestamp = time();
                $tempZipPath = storage_path('app') . DIRECTORY_SEPARATOR . "temp_report_{$timestamp}.zip";
                $extractPath = storage_path('app') . DIRECTORY_SEPARATOR . "temp_report_{$timestamp}";

                $client->get($downloadUrl, [
                    'sink' => $tempZipPath,
                ]);
                Log::info('Successfully downloaded the file from the provided URL.');
                Log::info('Download took: ' . (microtime(true) - $start) . ' seconds');

                $start = microtime(true);

                $storageAppPath = storage_path('app');
                if (!file_exists($storageAppPath)) {
                    mkdir($storageAppPath, 0775, true);
                    //  Log::info('Created directory: ' . $storageAppPath);
                }
                Log::info('Creating directory took: ' . (microtime(true) - $start) . ' seconds');

                //   Log::info('Saved zip file to: ' . $tempZipPath);
                $start = microtime(true);
                $zip = new \ZipArchive();
                if ($zip->open($tempZipPath) === true) {

                    $zip->extractTo($extractPath);
                    $zip->close();
                    //    Log::info('Extracted zip file to: ' . $extractPath);
                    Log::info('Extraction took: ' . (microtime(true) - $start) . ' seconds');
                    // Process the CSV files
                    $data = $this->processCsvFiles($extractPath, $selectedDate);
                    $this->buildFinalSummaryFromData($data, $selectedDate);

                    // Delete temporary files
                    unlink($tempZipPath);
                    // Optionally delete extracted files
                    $this->deleteDirectory($extractPath);

                    //    Log::info('Successfully processed CSV files.');

                    return true;
                } else {
                    Log::error('Failed to open zip file.');
                    return false;
                }
            } else {
                Log::error('Failed to retrieve the report file. ReportBlobUri not found in response body.');
                return false;
            }

        } catch (RequestException $e) {
            Log::error('Error downloading report: ' . $e->getMessage());
            return false;
        }
    }



    private function processCsvFiles($extractPath, $selectedDate)
    {
        // Log::info('Starting to process CSV files.');

        $csvFiles = [
            'Cash-Management' => 'processCashManagement',
            'SalesEntryForm-FinancialView' => 'processFinancialView',
            'Summary-Items' => 'processSummaryItems',
            'Summary-Sales' => 'processSummarySales',
            'Summary-Transactions' => 'processSummaryTransactions',
            'Detail-Orders' => 'processDetailOrders',
            'Waste-Report' => 'processWaste',
            'Detail-OrderLines' => 'processOrderLine'

        ];

        $allData = [];

        foreach ($csvFiles as $filePrefix => $processorMethod) {
            $fileNamePattern = $filePrefix . '-*_' . $selectedDate . '.csv';
            $files = glob($extractPath . DIRECTORY_SEPARATOR . $fileNamePattern);

            foreach ($files as $filePath) {
                Log::info('Processing file: ' . $filePath);
                $processed = $this->$processorMethod($filePath);
                $allData[$processorMethod] = array_merge($allData[$processorMethod] ?? [], $processed);
            }
        }

        return $allData; // data for summary building
    }

    /**
     * @param array{
     *   processCashManagement?: array,
     *   processFinancialView?: array,
     *   processSummaryItems?: array,
     *   processSummarySales?: array,
     *   processSummaryTransactions?: array,
     *   processDetailOrders?: array,
     *   processWaste?: array,
     *   processOrderline?: array,
     * } $data
     * @param string $selectedDate
     * @return void
     */

    private function buildFinalSummaryFromData(array $data, string $selectedDate): void
    {

        //  Log::info('Building final summary from in-memory data.');

        $detailOrder = collect($data['processDetailOrders'] ?? []);
        $financialView = collect($data['processFinancialView'] ?? []);
        $wasteData = collect($data['processWaste'] ?? []);
        $orderLine = collect($data['processOrderLine'] ?? []);

        $allFranchiseStores = collect([
            ...$detailOrder->pluck('franchise_store'),
            ...$financialView->pluck('franchise_store'),
            ...$wasteData->pluck('franchise_store')
        ])->unique();

        //this array for the channel data
        $metrics = [
            'Sales' => [
                '-' => ['column' => 'royalty_obligation', 'type' => 'sum'],
            ],
            'Gross_Sales' => [
                '-' => ['column' => 'gross_sales', 'type' => 'sum'],
            ],
            'Order_Count' => [
                '-' => ['column' => 'order_id', 'type' => 'count'], // count distinct order IDs
            ],
            'Tips' => [
                'DeliveryTip' => ['column' => 'delivery_tip', 'type' => 'sum'],
                'DeliveryTipTax' => ['column' => 'delivery_tip_tax', 'type' => 'sum'],
                'StoreTipAmount' => ['column' => 'store_tip_amount', 'type' => 'sum'],
            ],
            'Tax' => [
                'TaxableAmount' => ['column' => 'taxable_amount', 'type' => 'sum'],
                'NonTaxableAmount' => ['column' => 'non_taxable_amount', 'type' => 'sum'],
                'TaxExemptAmount' => ['column' => 'tax_exempt_amount', 'type' => 'sum'],
                'SalesTax' => ['column' => 'sales_tax', 'type' => 'sum'],
                'OccupationalTax' => ['column' => 'occupational_tax', 'type' => 'sum'],
            ],
            'Fee' => [
                'DeliveryFee' => ['column' => 'delivery_fee', 'type' => 'sum'],
                'DeliveryFeeTax' => ['column' => 'delivery_fee_tax', 'type' => 'sum'],
                'DeliveryServiceFee' => ['column' => 'delivery_service_fee', 'type' => 'sum'],
                'DeliveryServiceFeeTax' => ['column' => 'delivery_service_fee_tax', 'type' => 'sum'],
                'DeliverySmallOrderFee' => ['column' => 'delivery_small_order_fee', 'type' => 'sum'],
                'DeliverySmallOrderFeeTax' => ['column' => 'delivery_small_order_fee_tax', 'type' => 'sum'],
            ],
            'HNR' => [
                'HNROrdersCount' => ['column' => 'hnrOrder', 'type' => 'sum'],
            ],
            'portal' => [
                'PutInPortalOrdersCount' => ['column' => 'portal_used', 'type' => 'sum'],
                'PutInPortalOnTimeOrdersCount' => ['column' => 'put_into_portal_before_promise_time', 'type' => 'sum'],
            ],
        ];


        $summaryRows = [];
        $summaryDate = $selectedDate ?? now();

        foreach ($allFranchiseStores as $store) {

            $OrderRows = $detailOrder->where('franchise_store', $store);
            $financeRows = $financialView->where('franchise_store', $store);
            $wasteRows = $wasteData->where('franchise_store', $store);
            $storeOrderLines = $orderLine->where('franchise_store', $store);

            // ChannelData

            $groupedCombos = $OrderRows->groupBy(function ($row) {
                return $row['order_placed_method'] . '|' . $row['order_fulfilled_method'];
            });

            foreach ($groupedCombos as $comboKey => $methodOrders) {
                [$placedMethod, $fulfilledMethod] = explode('|', $comboKey);

                foreach ($metrics as $category => $subcats) {
                    foreach ($subcats as $subcat => $info) {
                        if ($info['type'] === 'sum') {
                            $amount = $methodOrders->sum(function ($row) use ($info) {
                                return floatval($row[$info['column']] ?? 0);
                            });
                        } elseif ($info['type'] === 'count') {
                            $amount = $methodOrders->unique('order_id')->count();
                        }
                        if ($amount != 0) {
                            $summaryRows[] = [
                                'store' => $store,
                                'date' => $summaryDate,
                                'category' => $category,
                                'sub_category' => $subcat,
                                'order_placed_method' => $placedMethod,
                                'order_fulfilled_method' => $fulfilledMethod,
                                'amount' => $amount,
                            ];
                        }
                    }
                }
            }
            // if (count($summaryRows) > 0) {
            //     foreach (array_chunk($summaryRows, 1000) as $batch) {
            //         ChannelData::insert($batch);
            //     }
            // }
            // end of channel data


            //******* Bread Boost Summary *********//

            $classicOrders = $storeOrderLines
                ->whereIn('menu_item_name', ['Classic Pepperoni', 'Classic Cheese'])
                ->whereIn('order_fulfilled_method', ['Register', 'Drive-Thru'])
                ->whereIn('order_placed_method', ['Phone', 'Register', 'Drive Thru'])
                ->pluck('order_id')
                ->unique();

            $classicOrdersCount = $classicOrders->count();

            $classicWithBreadCount = $storeOrderLines
                ->whereIn('order_id', $classicOrders)
                ->where('menu_item_name', 'Crazy Bread')
                ->pluck('order_id')
                ->unique()
                ->count();

            $OtherPizzaOrder = $storeOrderLines
                ->whereNotIn('item_id', [
                    '-1',
                    '6',
                    '7',
                    '8',
                    '9',
                    '101001',
                    '101002',
                    '101288',
                    '103044',
                    '202901',
                    '101289',
                    '204100',
                    '204200'
                ])
                ->whereIn('order_fulfilled_method', ['Register', 'Drive-Thru'])
                ->whereIn('order_placed_method', ['Phone', 'Register', 'Drive Thru'])
                ->pluck('order_id')
                ->unique();

            $OtherPizzaOrderCount = $OtherPizzaOrder->count();

            $OtherPizzaWithBreadCount = $storeOrderLines
                ->whereIn('order_id', $OtherPizzaOrder)
                ->where('menu_item_name', 'Crazy Bread')
                ->pluck('order_id')
                ->unique()
                ->count();

            //******* End Of Bread_Boost *********//


            //******* Online Discount Program *********//
            $discountOrders = $OrderRows
                ->where('employee', '')
                ->where('modification_reason', '<>', '');

            foreach ($discountOrders as $discountOrder) {
                OnlineDiscountProgram::updateOrCreate(
                    [
                        'franchise_store' => $store,
                        'date' => $selectedDate,
                        'order_id' => $discountOrder['order_id']
                    ],
                    [
                        'pay_type' => $discountOrder['payment_methods'],
                        'original_subtotal' => 0,
                        'modified_subtotal' => $discountOrder['royalty_obligation'],
                        'promo_code' => trim(explode(':', $discountOrder['modification_reason'])[1] ?? '')
                    ]
                );
            }

            //******* End Of Online Discount Program *********//

            //******* Delivery Order Summary *********//
            $Oreders_count = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Count();

            $RO = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Sum('royalty_obligation');




            $occupational_tax = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Sum('occupational_tax');

            $delivery_charges = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Sum('delivery_fee');

            $delivery_charges_Taxes = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Sum('delivery_fee_tax');

            $delivery_Service_charges = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Sum('delivery_service_fee');

            $delivery_Service_charges_Tax = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Sum('delivery_service_fee_tax');

            $delivery_small_order_charge = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Sum('delivery_small_order_fee');

            $delivery_small_order_charge_Tax = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Sum('delivery_small_order_fee_tax');

            // $Delivery_Late_Fee_Count = $OrderRows
            //     ->where('delivery_fee', '<>', 0)
            //     ->where('put_into_portal_before_promise_time', 'No')
            //     ->where('portal_eligible', 'Yes')
            //     ->whereIn('order_placed_method', ['Mobile', 'Website'])
            //     ->where('order_fulfilled_method', 'Delivery')
            //     ->count();

            // $delivery_late_charge = $Delivery_Late_Fee_Count * 0.5;



            $Delivery_Tip_Summary = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Sum('delivery_tip');

            $Delivery_Tip_Tax_Summary = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Sum('delivery_tip_tax');

            $total_taxes = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->Sum('sales_tax');
            // sales tax       delivery_service_fee_tax 4.57    -  delivery_fee_tax 0.9     0.48
            $tax = $total_taxes - $delivery_Service_charges_Tax - $delivery_charges_Taxes - $delivery_small_order_charge_Tax;

            $product_cost = $RO - ($delivery_Service_charges + $delivery_charges + $delivery_small_order_charge);

            $order_total = $RO + $total_taxes + $Delivery_Tip_Summary;



            // $tax = $total_taxes - $delivery_Service_charges_Tax;

            //******* End Of Delivery Order Summary *********//

            //*******3rd Party Marketplace Orders*********//
            $doordash_product_costs_Marketplace = $OrderRows
                ->where('order_placed_method', 'DoorDash')
                ->Sum('royalty_obligation');
            $ubereats_product_costs_Marketplace = $OrderRows
                ->where('order_placed_method', 'UberEats')
                ->Sum('royalty_obligation');
            $grubhub_product_costs_Marketplace = $OrderRows
                ->where('order_placed_method', 'Grubhub')
                ->Sum('royalty_obligation');

            $doordash_tax_Marketplace = $OrderRows
                ->where('order_placed_method', 'DoorDash')
                ->Sum('sales_tax');
            $ubereats_tax_Marketplace = $OrderRows
                ->where('order_placed_method', 'UberEats')
                ->Sum('sales_tax');
            $grubhub_tax_Marketplace = $OrderRows
                ->where('order_placed_method', 'Grubhub')
                ->Sum('sales_tax');

            $doordash_order_total_Marketplace = $OrderRows
                ->where('order_placed_method', 'DoorDash')
                ->Sum('gross_sales');
            $uberEats_order_total_Marketplace = $OrderRows
                ->where('order_placed_method', 'UberEats')
                ->Sum('gross_sales');
            $grubhub_order_total_Marketplace = $OrderRows
                ->where('order_placed_method', 'Grubhub')
                ->Sum('gross_sales');

            //******* End Of 3rd Party Marketplace Orders *********//

            //******* For finance data table *********//
            $Pizza_Carryout = $financeRows
                ->where('sub_account', 'Pizza - Carryout')
                ->sum('amount');

            $HNR_Carryout = $financeRows
                ->where('sub_account', 'HNR - Carryout')
                ->sum('amount');

            $Bread_Carryout = $financeRows
                ->where('sub_account', 'Bread - Carryout')
                ->sum('amount');

            $Wings_Carryout = $financeRows
                ->where('sub_account', 'Wings - Carryout')
                ->sum('amount');

            $Beverages_Carryout = $financeRows
                ->where('sub_account', 'Beverages - Carryout')
                ->sum('amount');

            $Other_Foods_Carryout = $financeRows
                ->where('sub_account', 'Other Foods - Carryout')
                ->sum('amount');

            $Side_Items_Carryout = $financeRows
                ->where('sub_account', 'Side Items - Carryout')
                ->sum('amount');

            $Side_Items_Carryout = $financeRows
                ->where('sub_account', 'Side Items - Carryout')
                ->sum('amount');

            $Pizza_Delivery = $financeRows
                ->where('sub_account', 'Pizza - Delivery')
                ->sum('amount');

            $HNR_Delivery = $financeRows
                ->where('sub_account', 'HNR - Delivery')
                ->sum('amount');

            $Bread_Delivery = $financeRows
                ->where('sub_account', 'Bread - Delivery')
                ->sum('amount');

            $Wings_Delivery = $financeRows
                ->where('sub_account', 'Wings - Delivery')
                ->sum('amount');

            $Beverages_Delivery = $financeRows
                ->where('sub_account', 'Beverages - Delivery')
                ->sum('amount');

            $Other_Foods_Delivery = $financeRows
                ->where('sub_account', 'Other Foods - Delivery')
                ->sum('amount');

            $Side_Items_Delivery = $financeRows
                ->where('sub_account', 'Side Items - Delivery')
                ->sum('amount');

            $Delivery_Charges = $financeRows
                ->where('sub_account', 'Delivery-Fees')
                ->sum('amount');

            $TOTAL_Net_Sales = $Pizza_Carryout + $HNR_Carryout + $Bread_Carryout + $Wings_Carryout + $Beverages_Carryout + $Other_Foods_Carryout + $Side_Items_Carryout + $Pizza_Delivery + $HNR_Delivery + $Bread_Delivery + $Wings_Delivery + $Beverages_Delivery + $Other_Foods_Delivery + $Side_Items_Delivery + $Delivery_Charges;

            //customer count calculated below

            $Gift_Card_Non_Royalty = $financeRows
                ->where('sub_account', 'Gift Card')
                ->sum('amount');
            $Total_Non_Royalty_Sales = $financeRows
                ->where('sub_account', 'Non-Royalty')
                ->sum('amount');

            $Total_Non_Delivery_Tips = $financeRows
                ->where('area', 'Store Tips')
                ->sum('amount');




            // $Sales_Tax_Food_Beverage = $OrderRows
            //     ->where('order_fulfilled_method', 'Register')
            //     ->sum('sales_tax');

            $Sales_Tax_Delivery = $OrderRows
                ->where('order_fulfilled_method', 'Delivery')
                ->sum('sales_tax');


            $TOTAL_Sales_TaxQuantity = $financeRows
                ->where('sub_account', 'Sales-Tax')
                ->sum('amount');

            $Sales_Tax_Food_Beverage = $TOTAL_Sales_TaxQuantity - $Sales_Tax_Delivery;


            $DELIVERY_Quantity = $OrderRows
                ->where('delivery_fee', '<>', 0)
                ->where('royalty_obligation', '!=', 0)
                ->count();

            $Delivery_Fee = $OrderRows->sum('delivery_fee');
            $Delivery_Service_Fee = $OrderRows->sum('delivery_service_fee');
            $Delivery_Small_Order_Fee = $OrderRows->sum('delivery_small_order_fee');
            $TOTAL_Native_App_Delivery_Fees = $financeRows
                ->where('sub_account', 'Delivery-Fees')
                ->sum('amount');

            $Delivery_Late_to_Portal_Fee_Count = $OrderRows
                ->where('delivery_fee', '!=', 0)
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('order_fulfilled_method', 'Delivery')
                ->filter(function ($order) {
                    $loadedRaw = trim((string) $order['time_loaded_into_portal'] ?? '');
                    $promiseRaw = trim((string) $order['promise_date'] ?? '');

                    if (empty($loadedRaw) || empty($promiseRaw)) {
                        return false;
                    }

                    try {
                        $loadedTime = Carbon::createFromFormat('Y-m-d H:i:s', $loadedRaw);
                        $promiseTimePlus5 = Carbon::createFromFormat('Y-m-d H:i:s', $promiseRaw)->addMinutes(5);

                        return $loadedTime->greaterThan($promiseTimePlus5);
                    } catch (\Exception $e) {
                        Log::warning('Late portal fee date parse failed', [
                            'loaded' => $loadedRaw,
                            'promise' => $promiseRaw,
                            'error' => $e->getMessage()
                        ]);
                        return false;
                    }
                })
                ->count();

            $Delivery_Late_to_Portal_Fee = round($Delivery_Late_to_Portal_Fee_Count * 0.5, 2);

            // Log::info('Late Portal Fee Summary', [
            //     'count' => $Delivery_Late_to_Portal_Fee_Count,
            //     'fee' => $Delivery_Late_to_Portal_Fee,
            // ]);

            $Delivery_Tips = $financeRows
                ->whereIn('sub_account', ['Delivery-Tips', 'Prepaid-Delivery-Tips'])
                ->sum('amount');

            $DoorDash_Quantity = $OrderRows
                ->where('order_placed_method', 'DoorDash')
                ->count();
            $DoorDash_Order_Total = $OrderRows
                ->where('order_placed_method', 'DoorDash')
                ->sum('royalty_obligation');

            $Grubhub_Quantity = $OrderRows
                ->where('order_placed_method', 'Grubhub')
                ->count();
            $Grubhub_Order_Total = $OrderRows
                ->where('order_placed_method', 'Grubhub')
                ->sum('royalty_obligation');

            $Uber_Eats_Quantity = $OrderRows
                ->where('order_placed_method', 'UberEats')
                ->count();
            $Uber_Eats_Order_Total = $OrderRows
                ->where('order_placed_method', 'UberEats')
                ->sum('royalty_obligation');

            $ONLINE_ORDERING_Mobile_Order_Quantity = $OrderRows
                ->where('order_placed_method', 'Mobile')
                ->where('royalty_obligation', '!=', 0)
                ->Count();
            $ONLINE_ORDERING_Online_Order_Quantity = $OrderRows
                ->where('order_placed_method', 'Website')
                ->where('royalty_obligation', '!=', 0)
                ->Count();
            // not found yet ONLINE_ORDERING_Pay_In_Store
            /*

              AI_Pre_Paid
              AI_Pay_InStore
            */
            $ONLINE_ORDERING_Pay_In_Store = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->whereIn('order_fulfilled_method', ['Register', 'Drive-Thru'])
                ->where('royalty_obligation', '!=', 0)
                ->Count();

            $Agent_Pre_Paid = $OrderRows
                ->where('order_placed_method', 'SoundHoundAgent')
                ->where('order_fulfilled_method', 'Delivery')
                ->where('royalty_obligation', '!=', 0)
                ->Count();

            $Agent_Pay_In_Store = $OrderRows
                ->where('order_placed_method', 'SoundHoundAgent')
                ->whereIn('order_fulfilled_method', ['Register', 'Drive-Thru'])
                ->where('royalty_obligation', '!=', 0)
                ->Count();

            $PrePaid_Cash_Orders = $financeRows
                ->where('sub_account', 'PrePaidCash-Orders')
                ->sum('amount');

            $PrePaid_Non_Cash_Orders = $financeRows
                ->where('sub_account', 'PrePaidNonCash-Orders')
                ->sum('amount');

            $PrePaid_Sales = $financeRows
                ->where('sub_account', 'PrePaid-Sales')
                ->sum('amount');

            $Prepaid_Delivery_Tips = $financeRows
                ->where('sub_account', 'Prepaid-Delivery-Tips')
                ->sum('amount');

            $Prepaid_InStore_Tips = $financeRows
                ->where('sub_account', 'Prepaid-InStoreTipAmount')
                ->sum('amount');

            $Marketplace_from_Non_Cash_Payments_box = $financeRows
                ->whereIn('sub_account', ['Marketplace - DoorDash', 'Marketplace - UberEats', 'Marketplace - Grubhub'])
                ->sum('amount');

            $AMEX = $financeRows
                ->whereIn('sub_account', ['Credit Card - AMEX', 'EPay - AMEX'])
                ->sum('amount');

            //Total_Non_Cash_Payments
            $credit_card_Cash_Payments = $financeRows
                ->whereIn('sub_account', ['Credit Card - Discover', 'Credit Card - AMEX', 'Credit Card - Visa/MC'])
                ->sum('amount');

            $Debit_Cash_Payments = $financeRows
                ->where('sub_account', 'Debit')
                ->sum('amount');

            $epay_Cash_Payments = $financeRows
                ->whereIn('sub_account', ['EPay - Visa/MC', 'EPay - AMEX', 'EPay - Discover'])
                ->sum('amount');

            $Total_Non_Cash_Payments = $financeRows
                ->where('sub_account', 'Non-Cash-Payments')
                ->sum('amount');
            //
            $Non_Cash_Payments = $Total_Non_Cash_Payments -
                $AMEX -
                $Marketplace_from_Non_Cash_Payments_box -
                $Gift_Card_Non_Royalty;


            //finance sheet

            $Cash_Sales = $financeRows
                ->where('sub_account', 'Cash-Check-Deposit')
                ->sum('amount');

            $Cash_Drop = $financeRows
                ->where('sub_account', 'Cash Drop Total')
                ->sum('amount');

            $Tip_Drop_Total = $financeRows
                ->where('sub_account', 'Tip Drop Total')
                ->sum('amount');



            $Over_Short = $financeRows
                ->where('sub_account', 'Over-Short-Operating')
                ->sum('amount');

                $Cash_Drop_Total = $Cash_Drop + $Over_Short;

            $Payouts = $financeRows
                ->where('sub_account', 'Payouts')
                ->sum('amount');

            //********  ********//

            // detail_orders (OrderRows)
            $totalSales = $OrderRows->sum('royalty_obligation');

            $modifiedOrderQty = $OrderRows->filter(function ($row) {
                return !empty(trim($row['override_approval_employee']));
            })->count();

            $RefundedOrderQty = $OrderRows
                ->where('refunded', "Yes")
                ->count();

            $customerCount = $OrderRows->sum('customer_count');

            $phoneSales = $OrderRows
                ->where('order_placed_method', 'Phone')
                ->sum('royalty_obligation');

            $callCenterAgent = $OrderRows
                ->where('order_placed_method', 'SoundHoundAgent')
                ->sum('royalty_obligation');

            $driveThruSales = $OrderRows
                ->where('order_placed_method', 'Drive Thru')
                ->sum('royalty_obligation');

            $websiteSales = $OrderRows
                ->where('order_placed_method', 'Website')
                ->sum('royalty_obligation');

            $mobileSales = $OrderRows
                ->where('order_placed_method', 'Mobile')
                ->sum('royalty_obligation');

            $doordashSales = $OrderRows
                ->where('order_placed_method', 'DoorDash')
                ->sum('royalty_obligation');

            $grubHubSales = $OrderRows
                ->where('order_placed_method', 'Grubhub')
                ->sum('royalty_obligation');

            $uberEatsSales = $OrderRows
                ->where('order_placed_method', 'UberEats')
                ->sum('royalty_obligation');

            $deliverySales = $doordashSales + $grubHubSales + $uberEatsSales + $mobileSales + $websiteSales;

            $digitalSales = $totalSales > 0
                ? ($deliverySales / $totalSales)
                : 0;

            $portalTransaction = $OrderRows
                ->where('portal_eligible', 'Yes')
                ->count();

            $putIntoPortal = $OrderRows
                ->where('portal_used', 'Yes')
                ->count();

            $portalPercentage = $portalTransaction > 0
                ? ($putIntoPortal / $portalTransaction)
                : 0;

            $portalOnTime = $OrderRows
                ->where('put_into_portal_before_promise_time', 'Yes')
                ->count();

            $inPortalPercentage = $portalTransaction > 0
                ? ($portalOnTime / $portalTransaction)
                : 0;
            // detail_orders (OrderRows) end

            $deliveryTips = $financeRows
                ->where('sub_account', 'Delivery-Tips')
                ->sum('amount');

            $prePaidDeliveryTips = $financeRows
                ->where('sub_account', 'Prepaid-Delivery-Tips')
                ->sum('amount');

            $inStoreTipAmount = $financeRows
                ->where('sub_account', 'InStoreTipAmount')
                ->sum('amount');

            $prePaidInStoreTipAmount = $financeRows
                ->where('sub_account', 'Prepaid-InStoreTipAmount')
                ->sum('amount');

            $totalTips = $deliveryTips + $prePaidDeliveryTips + $inStoreTipAmount + $prePaidInStoreTipAmount;

            $overShort = $financeRows
                ->where('sub_account', 'Over-Short')
                ->sum('amount');

            //final sum
            $cashSales = $financeRows
                ->where('sub_account', 'Total Cash Sales')
                ->sum('amount');

            $totalWasteCost = $wasteRows->sum(function ($row) {
                return $row['item_cost'] * $row['quantity'];
            });

            BreadBoostModel::updateOrCreate(

                ['franchise_store' => $store, 'date' => $selectedDate],
                [
                    'classic_order' => $classicOrdersCount,
                    'classic_with_bread' => $classicWithBreadCount,
                    'other_pizza_order' => $OtherPizzaOrderCount,
                    'other_pizza_with_bread' => $OtherPizzaWithBreadCount,
                ]
            );

            DeliveryOrderSummary::updateOrCreate(
                ['franchise_store' => $store, 'date' => $selectedDate],
                [
                    'orders_count' => $Oreders_count,
                    'product_cost' => $product_cost,
                    'tax' => $tax,
                    'occupational_tax' => $occupational_tax,
                    'delivery_charges' => $delivery_charges,
                    'delivery_charges_taxes' => $delivery_charges_Taxes,
                    'service_charges' => $delivery_Service_charges,
                    'service_charges_taxes' => $delivery_Service_charges_Tax,
                    'small_order_charge' => $delivery_small_order_charge,
                    'small_order_charge_taxes' => $delivery_small_order_charge_Tax,
                    'delivery_late_charge' => $Delivery_Late_to_Portal_Fee,
                    'tip' => $Delivery_Tip_Summary,
                    'tip_tax' => $Delivery_Tip_Tax_Summary,
                    'total_taxes' => $total_taxes,
                    'order_total' => $order_total

                ]
            );

            ThirdPartyMarketplaceOrder::updateOrCreate(
                ['franchise_store' => $store, 'date' => $selectedDate],
                [
                    'doordash_product_costs_Marketplace' => $doordash_product_costs_Marketplace,
                    'doordash_tax_Marketplace' => $doordash_tax_Marketplace,
                    'doordash_order_total_Marketplace' => $doordash_order_total_Marketplace,
                    'ubereats_product_costs_Marketplace' => $ubereats_product_costs_Marketplace,
                    'ubereats_tax_Marketplace' => $ubereats_tax_Marketplace,
                    'uberEats_order_total_Marketplace' => $uberEats_order_total_Marketplace,
                    'grubhub_product_costs_Marketplace' => $grubhub_product_costs_Marketplace,
                    'grubhub_tax_Marketplace' => $grubhub_tax_Marketplace,
                    'grubhub_order_total_Marketplace' => $grubhub_order_total_Marketplace,
                ]
            );

            // OnlineDiscountProgram::updateOrCreate(
            //     ['franchise_store' => $store, 'date' => $selectedDate],
            //     [
            //         'order_id'=> $Order_ID,
            //         'pay_type'=> $Pay_Type,
            //         'original_subtotal'=> $Original_Subtotal,
            //         'modified_subtotal'=> $Modified_Subtotal,
            //         'promo_code'=> $Promo_Code
            //     ]
            // );

            FinanceData::updateOrCreate(
                ['franchise_store' => $store, 'business_date' => $selectedDate],
                [
                    'Pizza_Carryout' => $Pizza_Carryout,
                    'HNR_Carryout' => $HNR_Carryout,
                    'Bread_Carryout' => $Bread_Carryout,
                    'Wings_Carryout' => $Wings_Carryout,
                    'Beverages_Carryout' => $Beverages_Carryout,
                    'Other_Foods_Carryout' => $Other_Foods_Carryout,
                    'Side_Items_Carryout' => $Side_Items_Carryout,
                    'Pizza_Delivery' => $Pizza_Delivery,
                    'HNR_Delivery' => $HNR_Delivery,
                    'Bread_Delivery' => $Bread_Delivery,
                    'Wings_Delivery' => $Wings_Delivery,
                    'Beverages_Delivery' => $Beverages_Delivery,
                    'Other_Foods_Delivery' => $Other_Foods_Delivery,
                    'Side_Items_Delivery' => $Side_Items_Delivery,
                    'Delivery_Charges' => $Delivery_Charges,
                    'TOTAL_Net_Sales' => $TOTAL_Net_Sales,
                    'Customer_Count' => $customerCount,
                    'Gift_Card_Non_Royalty' => $Gift_Card_Non_Royalty,
                    'Total_Non_Royalty_Sales' => $Total_Non_Royalty_Sales,
                    'Total_Non_Delivery_Tips' => $Total_Non_Delivery_Tips,
                    'Sales_Tax_Food_Beverage' => $Sales_Tax_Food_Beverage,
                    'Sales_Tax_Delivery' => $Sales_Tax_Delivery,
                    'TOTAL_Sales_TaxQuantity' => $TOTAL_Sales_TaxQuantity,
                    'DELIVERY_Quantity' => $DELIVERY_Quantity,
                    'Delivery_Fee' => $Delivery_Fee,
                    'Delivery_Service_Fee' => $Delivery_Service_Fee,
                    'Delivery_Small_Order_Fee' => $Delivery_Small_Order_Fee,
                    'Delivery_Late_to_Portal_Fee' => $Delivery_Late_to_Portal_Fee,
                    'TOTAL_Native_App_Delivery_Fees' => $TOTAL_Native_App_Delivery_Fees,
                    'Delivery_Tips' => $Delivery_Tips,
                    'DoorDash_Quantity' => $DoorDash_Quantity,
                    'DoorDash_Order_Total' => $DoorDash_Order_Total,
                    'Grubhub_Quantity' => $Grubhub_Quantity,
                    'Grubhub_Order_Total' => $Grubhub_Order_Total,
                    'Uber_Eats_Quantity' => $Uber_Eats_Quantity,
                    'Uber_Eats_Order_Total' => $Uber_Eats_Order_Total,
                    'ONLINE_ORDERING_Mobile_Order_Quantity' => $ONLINE_ORDERING_Mobile_Order_Quantity,
                    'ONLINE_ORDERING_Online_Order_Quantity' => $ONLINE_ORDERING_Online_Order_Quantity,
                    'ONLINE_ORDERING_Pay_In_Store' => $ONLINE_ORDERING_Pay_In_Store,
                    'Agent_Pre_Paid' => $Agent_Pre_Paid,
                    'Agent_Pay_InStore' => $Agent_Pay_In_Store,
                    'AI_Pre_Paid' => null,
                    'AI_Pay_InStore' => null,
                    'PrePaid_Cash_Orders' => $PrePaid_Cash_Orders,
                    'PrePaid_Non_Cash_Orders' => $PrePaid_Non_Cash_Orders,
                    'PrePaid_Sales' => $PrePaid_Sales,
                    'Prepaid_Delivery_Tips' => $Prepaid_Delivery_Tips,
                    'Prepaid_InStore_Tips' => $Prepaid_InStore_Tips,
                    'Marketplace_from_Non_Cash_Payments_box' => $Marketplace_from_Non_Cash_Payments_box,
                    'AMEX' => $AMEX,
                    'Total_Non_Cash_Payments' => $Total_Non_Cash_Payments,
                    'credit_card_Cash_Payments' => $credit_card_Cash_Payments,
                    'Debit_Cash_Payments' => $Debit_Cash_Payments,
                    'epay_Cash_Payments' => $epay_Cash_Payments,
                    'Non_Cash_Payments' => $Non_Cash_Payments,
                    'Cash_Sales' => $Cash_Sales,
                    'Cash_Drop_Total' => $Cash_Drop_Total,
                    'Over_Short' => $Over_Short,
                    'Payouts' => $Payouts,
                ]
            );


            FinalSummary::updateOrCreate(
                ['franchise_store' => $store, 'business_date' => $selectedDate],
                [
                    'total_sales' => $totalSales,
                    'modified_order_qty' => $modifiedOrderQty,
                    'refunded_order_qty' => $RefundedOrderQty,
                    'customer_count' => $customerCount,

                    'phone_sales' => $phoneSales,
                    'call_center_sales' => $callCenterAgent,
                    'drive_thru_sales' => $driveThruSales,
                    'website_sales' => $websiteSales,
                    'mobile_sales' => $mobileSales,

                    'doordash_sales' => $doordashSales,
                    'grubhub_sales' => $grubHubSales,
                    'ubereats_sales' => $uberEatsSales,
                    'delivery_sales' => $deliverySales,
                    'digital_sales_percent' => round($digitalSales, 2),

                    'portal_transactions' => $portalTransaction,
                    'put_into_portal' => $putIntoPortal,
                    'portal_used_percent' => round($portalPercentage, 2),
                    'put_in_portal_on_time' => $portalOnTime,
                    'in_portal_on_time_percent' => round($inPortalPercentage, 2),

                    'delivery_tips' => $deliveryTips,
                    'prepaid_delivery_tips' => $prePaidDeliveryTips,
                    'in_store_tip_amount' => $inStoreTipAmount,
                    'prepaid_instore_tip_amount' => $prePaidInStoreTipAmount,
                    'total_tips' => $totalTips,

                    'over_short' => $overShort,
                    'cash_sales' => $cashSales,


                    'total_waste_cost' => $totalWasteCost,
                ]
            );



            // Save hourly sales
            $ordersByHour = $OrderRows->groupBy(function ($order) {
                return Carbon::parse($order['promise_date'])->format('H');
            });

            foreach ($ordersByHour as $hour => $hourOrders) {
                HourlySales::updateOrCreate(
                    [
                        'franchise_store' => $store,
                        'business_date' => $selectedDate,
                        'hour' => (int) $hour,
                    ],
                    [
                        'total_sales' => $hourOrders->sum('royalty_obligation'),
                        'phone_sales' => $hourOrders->where('order_placed_method', 'Phone')->sum('royalty_obligation'),
                        'call_center_sales' => $hourOrders->where('order_placed_method', 'SoundHoundAgent')->sum('royalty_obligation'),
                        'drive_thru_sales' => $hourOrders->where('order_placed_method', 'Drive Thru')->sum('royalty_obligation'),
                        'website_sales' => $hourOrders->where('order_placed_method', 'Website')->sum('royalty_obligation'),
                        'mobile_sales' => $hourOrders->where('order_placed_method', 'Mobile')->sum('royalty_obligation'),
                        'order_count' => $hourOrders->count(),
                    ]
                );
            }

        }
        if (!empty($summaryRows)) {
            foreach (array_chunk($summaryRows, 1000) as $batch) {
                ChannelData::upsert(
                    $batch,
                    ['store', 'date', 'category', 'sub_category', 'order_placed_method', 'order_fulfilled_method'],
                    ['amount']
                );
            }
        }


        Log::info('Final summary from in-memory data completed.');
    }

 private function processCashManagement($filePath)
{
    $data = $this->readCsv($filePath);
    $rows = [];

    foreach ($data as $row) {
        $createDatetime = $this->parseDateTime($row['createdatetime']);
        $verifiedDatetime = $this->parseDateTime($row['verifieddatetime']);

        $rows[] = [
            'franchise_store' => $row['franchisestore'],
            'business_date' => $row['businessdate'],
            'create_datetime' => $createDatetime,
            'verified_datetime' => $verifiedDatetime,
            'till' => $row['till'],
            'check_type' => $row['checktype'],
            'system_totals' => $row['systemtotals'],
            'verified' => $row['verified'],
            'variance' => $row['variance'],
            'created_by' => $row['createdby'],
            'verified_by' => $row['verifiedby']
        ];
    }

    foreach (array_chunk($rows, 500) as $batch) {
        CashManagement::upsert(
            $batch,
            ['franchise_store', 'business_date', 'create_datetime', 'till', 'check_type'],
            ['verified_datetime', 'system_totals', 'verified', 'variance', 'created_by', 'verified_by']
        );
    }

    return $rows;
}


   private function processWaste($filePath)
{
    $data = $this->readCsv($filePath);
    $rows = [];

    foreach ($data as $row) {
        $wasteDateTime = $this->parseDateTime($row['wastedatetime']);
        $produceDateTime = $this->parseDateTime($row['producedatetime']);

        $rows[] = [
            'franchise_store' => $row['franchisestore'],
            'business_date' => $row['businessdate'],
            'cv_item_id' => $row['cvitemid'],
            'menu_item_name' => $row['menuitemname'],
            'expired' => strtolower($row['expired']) === 'yes',
            'waste_date_time' => $wasteDateTime,
            'produce_date_time' => $produceDateTime,
            'waste_reason' => $row['wastereason'] ?? null,
            'cv_order_id' => $row['cvorderid'] ?? null,
            'waste_type' => $row['wastetype'],
            'item_cost' => $row['itemcost'],
            'quantity' => $row['quantity'],
        ];
    }

    foreach (array_chunk($rows, 500) as $batch) {
        Waste::upsert(
            $batch,
            ['business_date', 'franchise_store', 'cv_item_id', 'waste_date_time'],
            ['menu_item_name', 'expired', 'produce_date_time', 'waste_reason', 'cv_order_id', 'waste_type', 'item_cost', 'quantity']
        );
    }

    return $rows;
}


    private function processFinancialView($filePath)
{
    $data = $this->readCsv($filePath);
    $rows = [];

    foreach ($data as $row) {
        $rows[] = [
            'franchise_store' => $row['franchisestore'],
            'business_date' => $row['businessdate'],
            'area' => $row['area'],
            'sub_account' => $row['subaccount'],
            'amount' => $row['amount'],
        ];
    }

    foreach (array_chunk($rows, 500) as $batch) {
        FinancialView::upsert(
            $batch,
            ['franchise_store', 'business_date', 'sub_account', 'area'],
            ['amount']
        );
    }

    return $rows;
}

   private function processSummaryItems($filePath)
{
    $data = $this->readCsv($filePath);
    $rows = [];

    foreach ($data as $row) {
        $rows[] = [
            'franchise_store' => $row['franchisestore'],
            'business_date' => $row['businessdate'],
            'menu_item_name' => $row['menuitemname'],
            'menu_item_account' => $row['menuitemaccount'],
            'item_id' => $row['itemid'],
            'item_quantity' => $row['itemquantity'],
            'royalty_obligation' => $row['royaltyobligation'],
            'taxable_amount' => $row['taxableamount'],
            'non_taxable_amount' => $row['nontaxableamount'],
            'tax_exempt_amount' => $row['taxexemptamount'],
            'non_royalty_amount' => $row['nonroyaltyamount'],
            'tax_included_amount' => $row['taxincludedamount']
        ];
    }

    foreach (array_chunk($rows, 500) as $batch) {
        SummaryItem::upsert(
            $batch,
            ['franchise_store', 'business_date', 'menu_item_name', 'item_id'],
            [
                'menu_item_account',
                'item_quantity',
                'royalty_obligation',
                'taxable_amount',
                'non_taxable_amount',
                'tax_exempt_amount',
                'non_royalty_amount',
                'tax_included_amount'
            ]
        );
    }

    return $rows;
}


   private function processSummarySales($filePath)
{
    $data = $this->readCsv($filePath);
    $rows = [];

    foreach ($data as $row) {
        $rows[] = [
            'franchise_store' => $row['franchisestore'],
            'business_date' => $row['businessdate'],
            'royalty_obligation' => $row['royaltyobligation'],
            'customer_count' => $row['customercount'],
            'taxable_amount' => $row['taxableamount'],
            'non_taxable_amount' => $row['nontaxableamount'],
            'tax_exempt_amount' => $row['taxexemptamount'],
            'non_royalty_amount' => $row['nonroyaltyamount'],
            'refund_amount' => $row['refundamount'],
            'sales_tax' => $row['salestax'],
            'gross_sales' => $row['grosssales'],
            'occupational_tax' => $row['occupationaltax'],
            'delivery_tip' => $row['deliverytip'],
            'delivery_fee' => $row['deliveryfee'],
            'delivery_service_fee' => $row['deliveryservicefee'],
            'delivery_small_order_fee' => $row['deliverysmallorderfee'],
            'modified_order_amount' => $row['modifiedorderamount'],
            'store_tip_amount' => $row['storetipamount'],
            'prepaid_cash_orders' => $row['prepaidcashorders'],
            'prepaid_non_cash_orders' => $row['prepaidnoncashorders'],
            'prepaid_sales' => $row['prepaidsales'],
            'prepaid_delivery_tip' => $row['prepaiddeliverytip'],
            'prepaid_in_store_tip_amount' => $row['prepaidinstoretipamount'],
            'over_short' => $row['overshort'],
            'previous_day_refunds' => $row['previousdayrefunds'],
            'saf' => $row['saf'],
            'manager_notes' => $row['managernotes']
        ];
    }

    foreach (array_chunk($rows, 500) as $batch) {
        SummarySale::upsert(
            $batch,
            ['franchise_store', 'business_date'],
            [
                'royalty_obligation',
                'customer_count',
                'taxable_amount',
                'non_taxable_amount',
                'tax_exempt_amount',
                'non_royalty_amount',
                'refund_amount',
                'sales_tax',
                'gross_sales',
                'occupational_tax',
                'delivery_tip',
                'delivery_fee',
                'delivery_service_fee',
                'delivery_small_order_fee',
                'modified_order_amount',
                'store_tip_amount',
                'prepaid_cash_orders',
                'prepaid_non_cash_orders',
                'prepaid_sales',
                'prepaid_delivery_tip',
                'prepaid_in_store_tip_amount',
                'over_short',
                'previous_day_refunds',
                'saf',
                'manager_notes'
            ]
        );
    }

    return $rows;
}


  private function processSummaryTransactions($filePath)
{
    $data = $this->readCsv($filePath);
    $rows = [];

    foreach ($data as $row) {
        $rows[] = [
            'franchise_store' => $row['franchisestore'],
            'business_date' => $row['businessdate'],
            'payment_method' => $row['paymentmethod'],
            'sub_payment_method' => $row['subpaymentmethod'],
            'total_amount' => $row['totalamount'],
            'saf_qty' => $row['safqty'],
            'saf_total' => $row['saftotal']
        ];
    }

    foreach (array_chunk($rows, 500) as $batch) {
        SummaryTransaction::upsert(
            $batch,
            ['franchise_store', 'business_date', 'payment_method', 'sub_payment_method'],
            [
                'total_amount',
                'saf_qty',
                'saf_total'
            ]
        );
    }

    return $rows;
}


   private function processDetailOrders($filePath)
{
    $data = $this->readCsv($filePath);
    $rows = [];

    foreach ($data as $row) {
        // Parse datetime fields with lowercase keys
        $dateTimePlaced = $this->parseDateTime($row['datetimeplaced']);
        $dateTimeFulfilled = $this->parseDateTime($row['datetimefulfilled']);
        $promiseDate = $this->parseDateTime($row['promisedate']);
        $timeLoadedIntoPortal = $this->parseDateTime($row['timeloadedintoportal']);

        $rows[] = [
            'franchise_store' => $row['franchisestore'],
            'business_date' => $row['businessdate'],
            'date_time_placed' => $dateTimePlaced,
            'date_time_fulfilled' => $dateTimeFulfilled,
            'royalty_obligation' => $row['royaltyobligation'],
            'quantity' => $row['quantity'],
            'customer_count' => $row['customercount'],
            'order_id' => $row['orderid'],
            'taxable_amount' => $row['taxableamount'],
            'non_taxable_amount' => $row['nontaxableamount'],
            'tax_exempt_amount' => $row['taxexemptamount'],
            'non_royalty_amount' => $row['nonroyaltyamount'],
            'sales_tax' => $row['salestax'],
            'employee' => $row['employee'],
            'gross_sales' => $row['grosssales'],
            'occupational_tax' => $row['occupationaltax'],
            'override_approval_employee' => $row['overrideapprovalemployee'],
            'order_placed_method' => $row['orderplacedmethod'],
            'delivery_tip' => $row['deliverytip'],
            'delivery_tip_tax' => $row['deliverytiptax'],
            'order_fulfilled_method' => $row['orderfulfilledmethod'],
            'delivery_fee' => $row['deliveryfee'],
            'modified_order_amount' => $row['modifiedorderamount'],
            'delivery_fee_tax' => $row['deliveryfeetax'],
            'modification_reason' => $row['modificationreason'],
            'payment_methods' => $row['paymentmethods'],
            'delivery_service_fee' => $row['deliveryservicefee'],
            'delivery_service_fee_tax' => $row['deliveryservicefeetax'],
            'refunded' => $row['refunded'],
            'delivery_small_order_fee' => $row['deliverysmallorderfee'],
            'delivery_small_order_fee_tax' => $row['deliverysmallorderfeetax'],
            'transaction_type' => $row['transactiontype'],
            'store_tip_amount' => $row['storetipamount'],
            'promise_date' => $promiseDate,
            'tax_exemption_id' => $row['taxexemptionid'],
            'tax_exemption_entity_name' => $row['taxexemptionentityname'],
            'user_id' => $row['userid'],
            'hnrOrder' => $row['hnrorder'],
            'broken_promise' => $row['brokenpromise'],
            'portal_eligible' => $row['portaleligible'],
            'portal_used' => $row['portalused'],
            'put_into_portal_before_promise_time' => $row['putintoportalbeforepromisetime'],
            'portal_compartments_used' => $row['portalcompartmentsused'],
            'time_loaded_into_portal' => $timeLoadedIntoPortal
        ];
    }

    foreach (array_chunk($rows, 500) as $batch) {
        DetailOrder::upsert(
            $batch,
            ['franchise_store', 'business_date', 'order_id'],
            [
                'date_time_placed',
                'date_time_fulfilled',
                'royalty_obligation',
                'quantity',
                'customer_count',
                'taxable_amount',
                'non_taxable_amount',
                'tax_exempt_amount',
                'non_royalty_amount',
                'sales_tax',
                'employee',
                'gross_sales',
                'occupational_tax',
                'override_approval_employee',
                'order_placed_method',
                'delivery_tip',
                'delivery_tip_tax',
                'order_fulfilled_method',
                'delivery_fee',
                'modified_order_amount',
                'delivery_fee_tax',
                'modification_reason',
                'payment_methods',
                'delivery_service_fee',
                'delivery_service_fee_tax',
                'refunded',
                'delivery_small_order_fee',
                'delivery_small_order_fee_tax',
                'transaction_type',
                'store_tip_amount',
                'promise_date',
                'tax_exemption_id',
                'tax_exemption_entity_name',
                'user_id',
                'hnrOrder',
                'broken_promise',
                'portal_eligible',
                'portal_used',
                'put_into_portal_before_promise_time',
                'portal_compartments_used',
                'time_loaded_into_portal'
            ]
        );
    }

    return $rows;
}




    private function processOrderLine($filePath)
{
    $data = $this->readCsv($filePath);
    $rows = [];

    foreach ($data as $row) {
        $dateTimePlaced = $this->parseDateTime($row['datetimeplaced']);
        $dateTimeFulfilled = $this->parseDateTime($row['datetimefulfilled']);

        $rows[] = [
            'franchise_store' => $row['franchisestore'],
            'business_date' => $row['businessdate'],
            'date_time_placed' => $dateTimePlaced,
            'date_time_fulfilled' => $dateTimeFulfilled,
            'net_amount' => $row['netamount'],
            'quantity' => $row['quantity'],
            'royalty_item' => $row['royaltyitem'],
            'taxable_item' => $row['taxableitem'],
            'order_id' => $row['orderid'],
            'item_id' => $row['itemid'],
            'menu_item_name' => $row['menuitemname'],
            'menu_item_account' => $row['menuitemaccount'],
            'bundle_name' => $row['bundlename'],
            'employee' => $row['employee'],
            'override_approval_employee' => $row['overrideapprovalemployee'],
            'order_placed_method' => $row['orderplacedmethod'],
            'order_fulfilled_method' => $row['orderfulfilledmethod'],
            'modified_order_amount' => $row['modifiedorderamount'],
            'modification_reason' => $row['modificationreason'],
            'payment_methods' => $row['paymentmethods'],
            'refunded' => $row['refunded'],
            'tax_included_amount' => $row['taxincludedamount']
        ];
    }

    foreach (array_chunk($rows, 500) as $batch) {
        OrderLine::upsert(
            $batch,
            ['franchise_store', 'business_date', 'order_id', 'item_id'],
            [
                'date_time_placed',
                'date_time_fulfilled',
                'net_amount',
                'quantity',
                'royalty_item',
                'taxable_item',
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
                'tax_included_amount'
            ]
        );
    }

    return $rows;
}


private function readCsv($filePath)
{
    $data = [];
    if (($handle = fopen($filePath, 'r')) !== false) {
        $header = fgetcsv($handle, 1000, ',');

        // Normalize header: lowercase and trim
        $normalizedHeader = array_map(function ($key) {
            return strtolower(trim($key));
        }, $header);

        while (($row = fgetcsv($handle, 1000, ',')) !== false) {
            if (count($row) == count($normalizedHeader)) {
                $normalizedRow = array_combine($normalizedHeader, $row);
                $data[] = $normalizedRow;
            }
        }
        fclose($handle);
    }
    return $data;
}


    private function generateNonce()
    {
        // Replicating the GetNonce() function in the Postman script
        $nonce = strtolower(bin2hex(random_bytes(16)));
        // Log::info('Generated nonce: ' . $nonce);
        return $nonce;
    }

    private function prepareRequestUrlForSignature($requestUrl)
    {
        // Replace any {{variable}} in the URL if necessary
        $requestUrl = preg_replace_callback('/{{(\w*)}}/', function ($matches) {
            return env($matches[1], '');
        }, $requestUrl);

        // Encode and lowercase the URL
        $encodedUrl = strtolower(rawurlencode($requestUrl));
        //  Log::info('Encoded request URL: ' . $encodedUrl);
        return $encodedUrl;
    }

    private function parseDateTime($dateTimeString)
    {
        if (empty($dateTimeString)) {
            return null;
        }

        // Normalize whitespace and remove trailing Z
        $normalized = preg_replace('/[\x{00A0}\s]+/u', ' ', trim(str_replace('Z', '', $dateTimeString)));

        $formats = [
            'Y-m-d\TH:i:s.u',     // ISO with microseconds
            'Y-m-d\TH:i:s',       // ISO without microseconds
            'm-d-Y h:i:s A',      // US format with dash
            'm/d/Y h:i:s A',      // US format with slash
            'n-j-Y h:i:s A',      // Variant with no leading zeros
            'n/j/Y h:i:s A',
        ];

        foreach ($formats as $format) {
            try {
                return Carbon::createFromFormat($format, $normalized)->format('Y-m-d H:i:s');
            } catch (\Exception $e) {
                // Try next
            }
        }

        // Fallback: Carbon::parse() as last resort
        try {
            return Carbon::parse($normalized)->format('Y-m-d H:i:s');
        } catch (\Exception $e) {
            Log::error('Error parsing datetime string: ' . $dateTimeString . ' - ' . $e->getMessage());
            return null;
        }
    }
    // Optional method to delete the extracted files
    private function deleteDirectory($dirPath)
    {
        if (!is_dir($dirPath)) {
            return;
        }
        $files = scandir($dirPath);
        foreach ($files as $file) {
            if ($file != '.' && $file != '..') {
                $fullPath = $dirPath . DIRECTORY_SEPARATOR . $file;
                if (is_dir($fullPath)) {
                    $this->deleteDirectory($fullPath);
                } else {
                    unlink($fullPath);
                }
            }
        }
        rmdir($dirPath);
    }
}
