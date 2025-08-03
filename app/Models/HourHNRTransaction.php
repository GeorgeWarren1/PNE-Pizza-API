<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Factories\HasFactory;

class HourHNRTransaction extends Model
{
    use HasFactory;

    protected $table = 'hour_HNR_transactions';

    protected $fillable = [
        'franchise_store',
        'business_date',
        'hour',
        'transactions',
        'promise_broken_transactions',
        'promise_broken_percentage',
    ];

    protected $casts = [
        'business_date' => 'date',

    ];
}
