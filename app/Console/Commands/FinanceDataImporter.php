<?php

namespace App\Console\Commands;

use App\Models\DetailOrder;
use App\Models\FinanceData;
use App\Models\FinancialView;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Carbon\Carbon;

class FinanceDataImporter extends Command
{
    protected $signature = 'app:finance-data-importer
        {--start-date= : Start date in Y-m-d format}
        {--end-date= : End date in Y-m-d format}
        {--store= : Specific franchise store to process}';

    protected $description = 'Import and update FinanceData from related models';

    public function handle()
    {
        $startDate = $this->option('start-date') ? Carbon::parse($this->option('start-date')) : '2020-01-01';
        $endDate = $this->option('end-date') ? Carbon::parse($this->option('end-date')) : Carbon::now();
        $specificStore = $this->option('store');

        $query = DB::table('financial_views as fv')
            ->join('detail_orders as do', function ($join) {
                $join->on('fv.franchise_store', '=', 'do.franchise_store')
                    ->on('fv.business_date', '=', 'do.business_date');
            })
            ->select('fv.franchise_store', 'fv.business_date')
            ->distinct();

        if ($specificStore) {
            $query->where('fv.franchise_store', $specificStore);
        }

        $query->whereBetween('fv.business_date', [$startDate, $endDate->format('Y-m-d')]);
        $records = $query->get();

        $this->info('Found ' . $records->count() . ' records to process...');
        $bar = $this->output->createProgressBar($records->count());
        $bar->start();

        foreach ($records as $record) {
            $franchiseStore = $record->franchise_store;
            $businessDate = $record->business_date;

            $financialViews = FinancialView::where('franchise_store', $franchiseStore)
                ->where('business_date', $businessDate)->get();
            $detailOrders = DetailOrder::where('franchise_store', $franchiseStore)
                ->where('business_date', $businessDate)->get();

            // Initialize data array with default values


            // =====================================================================
            // CUSTOM QUERIES AND FORMULAS SECTION
            // =====================================================================
            // All three models are available here:
            // - $finalSummary (single record)
            // - $financialViews (collection)
            // - $detailOrders (collection)

            //******* For finance data table *********//
            $Pizza_Carryout = $financialViews
                ->where('sub_account', 'Pizza - Carryout')
                ->sum('amount');
            $HNR_Carryout = $financialViews
                ->where('sub_account', 'HNR - Carryout')
                ->sum('amount');
            $Bread_Carryout = $financialViews
                ->where('sub_account', 'Bread - Carryout')
                ->sum('amount');
            $Wings_Carryout = $financialViews
                ->where('sub_account', 'Wings - Carryout')
                ->sum('amount');
            $Beverages_Carryout = $financialViews
                ->where('sub_account', 'Beverages - Carryout')
                ->sum('amount');
            $Other_Foods_Carryout = $financialViews
                ->where('sub_account', 'Other Foods - Carryout')
                ->sum('amount');
            $Side_Items_Carryout = $financialViews
                ->where('sub_account', 'Side Items - Carryout')
                ->sum('amount');
            $Pizza_Delivery = $financialViews
                ->where('sub_account', 'Pizza - Delivery')
                ->sum('amount');
            $HNR_Delivery = $financialViews
                ->where('sub_account', 'HNR - Delivery')
                ->sum('amount');
            $Bread_Delivery = $financialViews
                ->where('sub_account', 'Bread - Delivery')
                ->sum('amount');
            $Wings_Delivery = $financialViews
                ->where('sub_account', 'Wings - Delivery')
                ->sum('amount');
            $Beverages_Delivery = $financialViews
                ->where('sub_account', 'Beverages - Delivery')
                ->sum('amount');
            $Other_Foods_Delivery = $financialViews
                ->where('sub_account', 'Other Foods - Delivery')
                ->sum('amount');
            $Side_Items_Delivery = $financialViews
                ->where('sub_account', 'Side Items - Delivery')
                ->sum('amount');
            $Delivery_Charges = $financialViews
                ->where('sub_account', 'Delivery-Fees')
                ->sum('amount');

            $TOTAL_Net_Sales = $Pizza_Carryout + $HNR_Carryout + $Bread_Carryout +
                $Wings_Carryout + $Beverages_Carryout + $Other_Foods_Carryout +
                $Side_Items_Carryout + $Pizza_Delivery + $HNR_Delivery +
                $Bread_Delivery + $Wings_Delivery + $Beverages_Delivery +
                $Other_Foods_Delivery + $Side_Items_Delivery + $Delivery_Charges;

            $Customer_Count = $detailOrders->sum('customer_count');

            $Gift_Card_Non_Royalty = $financialViews
                ->where('sub_account', 'Gift Card')
                ->sum('amount');
            $Total_Non_Royalty_Sales = $financialViews
                ->where('sub_account', 'Non-Royalty')
                ->sum('amount');
            $Total_Non_Delivery_Tips = $financialViews
                ->where('area', 'Store Tips')
                ->sum('amount');

            // $Sales_Tax_Food_Beverage = $detailOrders
            //     ->where('order_fulfilled_method', 'Register')
            //     ->sum('sales_tax');

            $Sales_Tax_Delivery = $detailOrders
                ->where('order_fulfilled_method', 'Delivery')
                ->sum('sales_tax');

            $TOTAL_Sales_TaxQuantity = $financialViews
                ->where('sub_account', 'Sales-Tax')
                ->sum('amount');

            $Sales_Tax_Food_Beverage = $TOTAL_Sales_TaxQuantity - $Sales_Tax_Delivery;

            $DELIVERY_Quantity = $detailOrders
                ->where('delivery_fee', '<>', 0)
                ->where('royalty_obligation', '!=', 0)
                ->count();
            $Delivery_Fee = $detailOrders->sum('delivery_fee');
            $Delivery_Service_Fee = $detailOrders->sum('delivery_service_fee');
            $Delivery_Small_Order_Fee = $detailOrders->sum('delivery_small_order_fee');
            $TOTAL_Native_App_Delivery_Fees = $financialViews
                ->where('sub_account', 'Delivery-Fees')
                ->sum('amount');


            $Delivery_Late_Fee_Count = $detailOrders
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
                        // Log::warning('Late portal fee date parse failed', [
                        //     'loaded' => $loadedRaw,
                        //     'promise' => $promiseRaw,
                        //     'error' => $e->getMessage()
                        // ]);
                        return false;
                    }
                })
                ->count();
            $Delivery_Late_to_Portal_Fee = round($Delivery_Late_Fee_Count * 0.5, 2);


            $Delivery_Tips = $financialViews
                ->whereIn('sub_account', ['Delivery-Tips', 'Prepaid-Delivery-Tips'])
                ->sum('amount');

            $DoorDash_Quantity = $detailOrders
                ->where('order_placed_method', 'DoorDash')
                ->count();
            $DoorDash_Order_Total = $detailOrders
                ->where('order_placed_method', 'DoorDash')
                ->sum('royalty_obligation');

            $Grubhub_Quantity = $detailOrders
                ->where('order_placed_method', 'Grubhub')
                ->count();
            $Grubhub_Order_Total = $detailOrders
                ->where('order_placed_method', 'Grubhub')
                ->sum('royalty_obligation');

            $Uber_Eats_Quantity = $detailOrders
                ->where('order_placed_method', 'UberEats')
                ->count();
            $Uber_Eats_Order_Total = $detailOrders
                ->where('order_placed_method', 'UberEats')
                ->sum('royalty_obligation');

            $ONLINE_ORDERING_Mobile_Order_Quantity = $detailOrders
                ->where('order_placed_method', 'Mobile')
                ->where('royalty_obligation', '!=', 0)
                ->count();
            $ONLINE_ORDERING_Online_Order_Quantity = $detailOrders
                ->where('order_placed_method', 'Website')
                ->where('royalty_obligation', '!=', 0)
                ->count();

            // Agent and AI fields - not found yet in the original code
            $ONLINE_ORDERING_Pay_In_Store = $detailOrders
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->whereIn('order_fulfilled_method', ['Register', 'Drive-Thru'])
                ->where('royalty_obligation', '!=', 0)
                ->Count();

            $Agent_Pre_Paid = $detailOrders
                ->where('order_placed_method', 'SoundHoundAgent')
                ->where('order_fulfilled_method', 'Delivery')
                ->where('royalty_obligation', '!=', 0)
                ->Count();

            $Agent_Pay_In_Store = $detailOrders
                ->where('order_placed_method', 'SoundHoundAgent')
                ->whereIn('order_fulfilled_method', ['Register', 'Drive-Thru'])
                ->where('royalty_obligation', '!=', 0)
                ->Count();


            $AI_Pre_Paid = 0;
            $AI_Pay_InStore = 0;

            $PrePaid_Cash_Orders = $financialViews
                ->where('sub_account', 'PrePaidCash-Orders')
                ->sum('amount');
            $PrePaid_Non_Cash_Orders = $financialViews
                ->where('sub_account', 'PrePaidNonCash-Orders')
                ->sum('amount');
            $PrePaid_Sales = $financialViews
                ->where('sub_account', 'PrePaid-Sales')
                ->sum('amount');
            $Prepaid_Delivery_Tips = $financialViews
                ->where('sub_account', 'Prepaid-Delivery-Tips')
                ->sum('amount');
            $Prepaid_InStore_Tips = $financialViews
                ->where('sub_account', 'Prepaid-InStoreTipAmount')
                ->sum('amount');

            $Marketplace_from_Non_Cash_Payments_box = $financialViews
                ->whereIn('sub_account', ['Marketplace - DoorDash', 'Marketplace - UberEats', 'Marketplace - Grubhub'])
                ->sum('amount');

            $AMEX = $financialViews
                ->whereIn('sub_account', ['Credit Card - AMEX', 'EPay - AMEX'])
                ->sum('amount');

            $credit_card_Cash_Payments = $financialViews
                ->whereIn('sub_account', ['Credit Card - Discover', 'Credit Card - AMEX', 'Credit Card - Visa/MC'])
                ->sum('amount');
            $Debit_Cash_Payments = $financialViews
                ->where('sub_account', 'Debit')
                ->sum('amount');
            $epay_Cash_Payments = $financialViews
                ->whereIn('sub_account', ['EPay - Visa/MC', 'EPay - AMEX', 'EPay - Discover'])
                ->sum('amount');

            $Total_Non_Cash_Payments = $financialViews
                ->where('sub_account', 'Non-Cash-Payments')
                ->sum('amount');

            $Non_Cash_Payments = $Total_Non_Cash_Payments -
                $AMEX -
                $Marketplace_from_Non_Cash_Payments_box -
                $Gift_Card_Non_Royalty;


            $Cash_Sales = $financialViews
                ->where('sub_account', 'Cash-Check-Deposit')
                ->sum('amount');

            $Cash_Drop = $financialViews
                ->where('sub_account', 'Cash Drop Total')
                ->sum('amount');

            $Tip_Drop_Total = $financialViews
                ->where('sub_account', 'Tip Drop Total')
                ->sum('amount');


            $Cash_Drop_Total = $Cash_Drop + $Tip_Drop_Total;
            //check this




            $Over_Short = $financialViews
                ->where('sub_account', 'Over-Short-Operating')
                ->sum('amount');


                $Cash_Drop_Total = $Cash_Drop + $Over_Short;

            $Payouts = $financialViews
                ->where('sub_account', 'Payouts')
                ->sum('amount');

            // Assign all calculated values to the data array
            $data['Pizza_Carryout'] = $Pizza_Carryout;
            $data['HNR_Carryout'] = $HNR_Carryout;
            $data['Bread_Carryout'] = $Bread_Carryout;
            $data['Wings_Carryout'] = $Wings_Carryout;
            $data['Beverages_Carryout'] = $Beverages_Carryout;
            $data['Other_Foods_Carryout'] = $Other_Foods_Carryout;
            $data['Side_Items_Carryout'] = $Side_Items_Carryout;
            $data['Pizza_Delivery'] = $Pizza_Delivery;
            $data['HNR_Delivery'] = $HNR_Delivery;
            $data['Bread_Delivery'] = $Bread_Delivery;
            $data['Wings_Delivery'] = $Wings_Delivery;
            $data['Beverages_Delivery'] = $Beverages_Delivery;
            $data['Other_Foods_Delivery'] = $Other_Foods_Delivery;
            $data['Side_Items_Delivery'] = $Side_Items_Delivery;
            $data['Delivery_Charges'] = $Delivery_Charges;
            $data['TOTAL_Net_Sales'] = $TOTAL_Net_Sales;
            $data['Customer_Count'] = $Customer_Count;
            $data['Gift_Card_Non_Royalty'] = $Gift_Card_Non_Royalty;
            $data['Total_Non_Royalty_Sales'] = $Total_Non_Royalty_Sales;
            $data['Total_Non_Delivery_Tips'] = $Total_Non_Delivery_Tips;

            $data['Sales_Tax_Food_Beverage'] = $Sales_Tax_Food_Beverage;
            $data['Sales_Tax_Delivery'] = $Sales_Tax_Delivery;
            $data['TOTAL_Sales_TaxQuantity'] = $TOTAL_Sales_TaxQuantity;

            $data['DELIVERY_Quantity'] = $DELIVERY_Quantity;
            $data['Delivery_Fee'] = $Delivery_Fee;
            $data['Delivery_Service_Fee'] = $Delivery_Service_Fee;
            $data['Delivery_Small_Order_Fee'] = $Delivery_Small_Order_Fee;
            $data['Delivery_Late_to_Portal_Fee'] = $Delivery_Late_to_Portal_Fee;
            $data['TOTAL_Native_App_Delivery_Fees'] = $TOTAL_Native_App_Delivery_Fees;
            $data['Delivery_Tips'] = $Delivery_Tips;
            $data['DoorDash_Quantity'] = $DoorDash_Quantity;
            $data['DoorDash_Order_Total'] = $DoorDash_Order_Total;
            $data['Grubhub_Quantity'] = $Grubhub_Quantity;
            $data['Grubhub_Order_Total'] = $Grubhub_Order_Total;
            $data['Uber_Eats_Quantity'] = $Uber_Eats_Quantity;
            $data['Uber_Eats_Order_Total'] = $Uber_Eats_Order_Total;
            $data['ONLINE_ORDERING_Mobile_Order_Quantity'] = $ONLINE_ORDERING_Mobile_Order_Quantity;
            $data['ONLINE_ORDERING_Online_Order_Quantity'] = $ONLINE_ORDERING_Online_Order_Quantity;
            $data['ONLINE_ORDERING_Pay_In_Store'] = $ONLINE_ORDERING_Pay_In_Store;
            $data['Agent_Pre_Paid'] = $Agent_Pre_Paid;
            $data['Agent_Pay_InStore'] = $Agent_Pay_In_Store;
            $data['AI_Pre_Paid'] = $AI_Pre_Paid;
            $data['AI_Pay_InStore'] = $AI_Pay_InStore;
            $data['PrePaid_Cash_Orders'] = $PrePaid_Cash_Orders;
            $data['PrePaid_Non_Cash_Orders'] = $PrePaid_Non_Cash_Orders;
            $data['PrePaid_Sales'] = $PrePaid_Sales;
            $data['Prepaid_Delivery_Tips'] = $Prepaid_Delivery_Tips;
            $data['Prepaid_InStore_Tips'] = $Prepaid_InStore_Tips;
            $data['Marketplace_from_Non_Cash_Payments_box'] = $Marketplace_from_Non_Cash_Payments_box;
            $data['AMEX'] = $AMEX;
            $data['Total_Non_Cash_Payments'] = $Total_Non_Cash_Payments;
            $data['credit_card_Cash_Payments'] = $credit_card_Cash_Payments;
            $data['Debit_Cash_Payments'] = $Debit_Cash_Payments;
            $data['epay_Cash_Payments'] = $epay_Cash_Payments;
            $data['Non_Cash_Payments'] = $Non_Cash_Payments;
            $data['Cash_Sales'] = $Cash_Sales;
            $data['Cash_Drop_Total'] = $Cash_Drop_Total;
            $data['Over_Short'] = $Over_Short;
            $data['Payouts'] = $Payouts;
            // =====================================================================
            // END OF CUSTOM QUERIES AND FORMULAS SECTION
            // =====================================================================
            $data = [
                'franchise_store' => $franchiseStore,
                'business_date' => $businessDate,
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
                'Customer_Count' => $Customer_Count,
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
                'AI_Pre_Paid' => $AI_Pre_Paid,
                'AI_Pay_InStore' => $AI_Pay_InStore,
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
            ];

            $keys = [
                'franchise_store' => $franchiseStore,
                'business_date' => $businessDate,
            ];

            // Create new FinanceData record
            FinanceData::updateOrCreate($keys, $data);


            $bar->advance();
        }

        $bar->finish();
        $this->newLine();
        $this->info('Finance data import completed successfully!');
    }
}
