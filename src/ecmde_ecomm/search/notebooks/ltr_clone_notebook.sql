-- Databricks notebook source

%sql
CREATE OR REPLACE TABLE ${ltr_destination_catalog}.${ltr_destination_schema}.ltr_abb_atc_rate_hist
    CLONE ${ltr_source_catalog}.${ltr_source_schema}.ltr_abb_atc_rate;


CREATE OR REPLACE TABLE ${ltr_destination_catalog}.${ltr_destination_schema}.ltr_abb_order_rate_hist
    CLONE ${ltr_source_catalog}.${ltr_source_schema}.ltr_abb_order_rate;


CREATE OR REPLACE TABLE ${ltr_destination_catalog}.${ltr_destination_schema}.ltr_abb_ctr_hist
    CLONE ${ltr_source_catalog}.${ltr_source_schema}.ltr_abb_ctr;

CREATE OR REPLACE TABLE ${ltr_destination_catalog}.${ltr_destination_schema}.ltr_abb_positive_profit_rate_hist
    CLONE ${ltr_source_catalog}.${ltr_source_schema}.ltr_abb_positive_profit_rate;
