<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class StoreHNRTransaction extends Model
{
    use HasFactory;

    protected $table = 'store_HNR_transactions';

    protected $fillable = [
        'franchise_store',
        'business_date',
        'item_id',
        'item_name',
        'transactions',
        'promise_met_transactions',
        'promise_met_percentage',
    ];

    protected $casts = [
        'business_date' => 'date',
        'promise_met_percentage' => 'decimal:2',
    ];
}
