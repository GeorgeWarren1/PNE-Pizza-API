<?php


use Illuminate\Support\Facades\Route;
use App\Http\Middleware\ApiKeyMiddleware;
use App\Http\Controllers\Data\ExportFinanceController;
use App\Http\Controllers\Data\ExportBreadBoostController;
use App\Http\Controllers\Data\ExportChannelDataController;
use App\Http\Controllers\Data\ExportDeliveryOrderSummaryController;
use App\Http\Controllers\Data\OnlineDiscountProgramExporterController;
use App\Http\Controllers\Data\ThirdPartyMarketplaceOrderExporter;
use App\Http\Controllers\Data\ExportOrderLine;

use App\Http\Controllers\Data\ExportController;
Route::get('/final-summary-json/{start_date?}/{end_date?}/{franchise_store?}', [ExportController::class, 'getFinalSummaryJson']);
Route::get('/final-summary-csv/{start_date?}/{end_date?}/{franchise_store?}', [ExportController::class, 'exportFinalSummaryCsv']);

// Routes for ExportHourlySalesController
use App\Http\Controllers\Data\ExportHourlySalesController;
Route::get('/hourly-sales-json/{start_date?}/{end_date?}/{franchise_store?}', [ExportHourlySalesController::class, 'getHourlySalesJson']);
Route::get('/hourly-sales-csv/{start_date?}/{end_date?}/{franchise_store?}', [ExportHourlySalesController::class, 'exportHourlySalesCsv']);

//Upselling
use App\Http\Controllers\Data\ExportUpsellingController;
Route::get('/upselling-summary-json/{start_date?}/{end_date?}/{franchise_store?}', [ExportUpsellingController::class, 'getUpsellingJson']);
Route::get('/upselling-summary-csv/{start_date?}/{end_date?}/{franchise_store?}', [ExportUpsellingController::class, 'exportUpsellingCsv']);
// Finance Data Export Routes
Route::get('/finance/export-csv/{start_date?}/{end_date?}/{franchise_store?}', [ExportFinanceController::class, 'exportCSVs']);
Route::get('/finance/export-json/{start_date?}/{end_date?}/{franchise_store?}', [ExportFinanceController::class, 'exportJson']);


// Bread Boost Export Routes
Route::get('/export/bread-boost/CSv/{start_date?}/{end_date?}/{franchise_store?}', [ExportBreadBoostController::class, 'exportCSV']);
Route::get('/export/bread-boost/json/{start_date?}/{end_date?}/{franchise_store?}', [ExportBreadBoostController::class, 'exportJson']);

//channel data
Route::get('export/channel-data/csv/{startDate?}/{endDate?}/{franchiseStores?}',[ExportChannelDataController::class, 'exportCSVs']);
Route::get('export/channel-data/json/{startDate?}/{endDate?}/{franchiseStores?}',[ExportChannelDataController::class, 'exportJson']);


//delivery
Route::get('/export/delivery-order-summary/csv/{startDate?}/{endDate?}/{franchiseStore?}', [ExportDeliveryOrderSummaryController::class, 'exportCSV']);
Route::get('/export/delivery-order-summary/json/{startDate?}/{endDate?}/{franchiseStore?}', [ExportDeliveryOrderSummaryController::class, 'exportJson']);

//online discount program
Route::get('/export/online-discount-program/csv/{startDate?}/{endDate?}/{franchiseStore?}', [OnlineDiscountProgramExporterController::class, 'exportCSV']);
Route::get('/export/online-discount-program/json/{startDate?}/{endDate?}/{franchiseStore?}', [OnlineDiscountProgramExporterController::class, 'exportJson']);

//third party
Route::get('/export/third-party-marketplace-orders/csv/{startDate?}/{endDate?}/{franchiseStore?}', [ThirdPartyMarketplaceOrderExporter::class, 'exportCSV']);
Route::get('/export/third-party-marketplace-orders/json/{startDate?}/{endDate?}/{franchiseStore?}', [ThirdPartyMarketplaceOrderExporter::class, 'exportJson']);




Route::middleware(ApiKeyMiddleware::class)->group(function () {
    Route::get('/export-upselling-summary/{start_date?}/{end_date?}/{franchise_store?}', [ExportUpsellingController::class, 'exportUpselling']);

    Route::get('/hourly-sales/{start_date?}/{end_date?}/{hours?}/{franchise_store?}', [ExportHourlySalesController::class, 'exportHourlySales']);

    Route::get('/export-final-summary/{start_date?}/{end_date?}/{franchise_store?}', [ExportController::class, 'exportFinalSummary']);
    Route::get('/finance/export-excel/{start_date?}/{end_date?}/{franchise_store?}', [ExportFinanceController::class, 'exportToExcel']);
    Route::get('/export/bread-boost/excel/{start_date?}/{end_date?}/{franchise_store?}', [ExportBreadBoostController::class, 'exportExcel']);

    Route::get('export/channel-data/excel/{StartDate?}/{EndDate?}/{franchiseStores?}',  [ExportChannelDataController::class, 'exportToExcel']);
    Route::get('/export/delivery-order-summary/excel/{startDate?}/{endDate?}/{franchiseStore?}', [ExportDeliveryOrderSummaryController::class, 'exportToExcel']);
    Route::get('/export/online-discount-program/excel/{startDate?}/{endDate?}/{franchiseStore?}', [OnlineDiscountProgramExporterController::class, 'exportToExcel']);
    Route::get('/export/third-party-marketplace-orders/excel/{startDate?}/{endDate?}/{franchiseStore?}', [ThirdPartyMarketplaceOrderExporter::class, 'exportToExcel']);

    Route::get('/order-line/export-excel/{start_date?}/{end_date?}/{franchise_store?}', [ExportOrderLine::class, 'exportcsv']);
});

