<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

class CreateStoreHNRTransactionsTable extends Migration
{
    public function up(): void
    {
        Schema::create('store_HNR_transactions', function (Blueprint $table) {
            $table->id();
            $table->string('franchise_store');
            $table->date('business_date');
            $table->string('item_id');
            $table->string('item_name');
            $table->integer('transactions')->default(0);
            $table->integer('promise_met_transactions')->default(0);
            $table->decimal('promise_met_percentage', 5, 2)->default(0.00);
            $table->timestamps();

            $table->unique(['franchise_store', 'business_date', 'item_id'], 'store_hnr_unique');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('store_HNR_transactions');
    }
}
