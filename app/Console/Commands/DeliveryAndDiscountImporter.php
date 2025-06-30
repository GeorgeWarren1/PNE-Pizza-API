<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Models\DeliveryOrderSummary;
use App\Models\OnlineDiscountProgram;
use App\Models\ThirdPartyMarketplaceOrder;
use App\Models\DetailOrder;
use Carbon\Carbon;
use Illuminate\Support\Facades\DB;

class DeliveryAndDiscountImporter extends Command
{
    protected $signature = 'report:delivery-discount {--start-date= : Start date (Y-m-d)} {--end-date= : End date (Y-m-d)}';
    protected $description = 'Process delivery and discount data for all stores and dates';

    public function handle()
    {
        $startDate = $this->option('start-date') ? Carbon::parse($this->option('start-date')) : null;
        $endDate = $this->option('end-date') ? Carbon::parse($this->option('end-date')) : null;

        $query = DetailOrder::select('business_date', 'franchise_store')
            ->distinct();

        if ($startDate) {
            $query->where('business_date', '>=', $startDate);
        }
        if ($endDate) {
            $query->where('business_date', '<=', $endDate);
        }

        $dates = $query->get();

        $bar = $this->output->createProgressBar($dates->count());
        $bar->start();

        foreach ($dates as $record) {
            $date = $record->business_date;
            $store = $record->franchise_store;

            $OrderRows = DetailOrder::where('business_date', $date)
                ->where('franchise_store', $store)
                ->get();

            // Convert to collection for better performance
            $OrderRows = collect($OrderRows);

            // Online Discount Program calculations
            $discountOrders = $OrderRows
                ->where('employee', '')
                ->where('modification_reason', '<>', '');

            foreach ($discountOrders as $discountOrder) {
                OnlineDiscountProgram::updateOrCreate(
                    [
                        'franchise_store' => $store,
                        'date' => $date,
                        'order_id' => $discountOrder['order_id']
                    ],
                    [
                        'pay_type' => $discountOrder['payment_methods'],
                        'original_subtotal' => 0,
                        'modified_subtotal' => $discountOrder['royalty_obligation'],
                        'promo_code' => trim(explode(':', $discountOrder['modification_reason'])[1] ?? '')
                    ]
                );

                // OnlineDiscountProgram::updateOrCreate(
                //     ['franchise_store' => $store, 'date' => $date],
                //     [
                //         'order_id' => $Order_ID,
                //         'pay_type' => $Pay_Type,
                //         'original_subtotal' => $Original_Subtotal,
                //         'modified_subtotal' => $Modified_Subtotal,
                //         'promo_code' => $Promo_Code
                //     ]
                // );
            }

            $Order_ID = $discountOrders->value('order_id');
            $Pay_Type = $discountOrders->value('payment_methods');
            $Original_Subtotal = 0;
            $Modified_Subtotal = $discountOrders->value('royalty_obligation');
            $Promo_Code_V = $discountOrders->value('modification_reason');
            $Promo_Code = trim(explode(':', $Promo_Code_V)[1] ?? '');

            // Delivery Order Summary calculations
            $deliveryOrders = $OrderRows
                ->whereIn('order_placed_method', ['Mobile', 'Website'])
                ->where('royalty_obligation', '!=', 0)
                ->where('order_fulfilled_method', 'Delivery');


            $Oreders_count = $deliveryOrders->count();

            $RO = $deliveryOrders->Sum('royalty_obligation');


            //$product_cost = 0;

            $occupational_tax = $deliveryOrders->sum('occupational_tax');
            $delivery_charges = $deliveryOrders->sum('delivery_fee');
            $delivery_charges_Taxes = $deliveryOrders->sum('delivery_fee_tax');
            $delivery_Service_charges = $deliveryOrders->sum('delivery_service_fee');
            $delivery_Service_charges_Tax = $deliveryOrders->sum('delivery_service_fee_tax');
            $delivery_small_order_charge = $deliveryOrders->sum('delivery_small_order_fee');
            $delivery_small_order_charge_Tax = $deliveryOrders->sum('delivery_small_order_fee_tax');

            $Delivery_Late_Fee_Count = $deliveryOrders
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

            $Delivery_Tip_Summary = $deliveryOrders->sum('delivery_tip');
            $Delivery_Tip_Tax_Summary = $deliveryOrders->sum('delivery_tip_tax');
            $total_taxes = $deliveryOrders->sum('sales_tax');


            $product_cost = $RO - ($delivery_Service_charges + $delivery_charges + $delivery_small_order_charge);

            $order_total = $RO + $total_taxes + $Delivery_Tip_Summary;

            $tax = $total_taxes - $delivery_Service_charges_Tax - $delivery_charges_Taxes - $delivery_small_order_charge_Tax;

            // Third Party Marketplace calculations
            $doordash_product_costs_Marketplace = $OrderRows->where('order_placed_method', 'DoorDash')->sum('royalty_obligation');
            $ubereats_product_costs_Marketplace = $OrderRows->where('order_placed_method', 'UberEats')->sum('royalty_obligation');
            $grubhub_product_costs_Marketplace = $OrderRows->where('order_placed_method', 'Grubhub')->sum('royalty_obligation');

            $doordash_tax_Marketplace = $OrderRows->where('order_placed_method', 'DoorDash')->sum('sales_tax');
            $ubereats_tax_Marketplace = $OrderRows->where('order_placed_method', 'UberEats')->sum('sales_tax');
            $grubhub_tax_Marketplace = $OrderRows->where('order_placed_method', 'Grubhub')->sum('sales_tax');

            $doordash_order_total_Marketplace = $OrderRows->where('order_placed_method', 'DoorDash')->sum('gross_sales');
            $uberEats_order_total_Marketplace = $OrderRows->where('order_placed_method', 'UberEats')->sum('gross_sales');
            $grubhub_order_total_Marketplace = $OrderRows->where('order_placed_method', 'Grubhub')->sum('gross_sales');

            // Save the data
            DeliveryOrderSummary::updateOrCreate(
                ['franchise_store' => $store, 'date' => $date],
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
                ['franchise_store' => $store, 'date' => $date],
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



            $bar->advance();
        }

        $bar->finish();
        $this->info("\nProcessing completed successfully");
    }
}
