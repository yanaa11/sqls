-- PostgreSQL

-- Subscribtions can be annual and monthly and can be pain in any day of month
-- So I divide every payment by days in billing period
-- And multiply "payment per day" by days in period where I calculate MRR

with 
refunds as 
(
	select 
		(t."attributes"::json ->> 'invoice_id')::int as invoice_id,
		t.orderid as order_id,
		sum((t."attributes"::json ->> 'amount')::float) as refund_amount
	from transactions t 
	where t.status = 'Refund'
	and (t."attributes"::json ->> 'amount')::float > 0
	and (t."attributes"::json ->> 'invoice_id')::int is not null
	group by t.orderid, invoice_id
),

self_service_invoices as
(	
	select 
		o.uid as order_id,
		o.accountid as account_id,
		o.productid as product_id,
		o.status as order_status,
		o.created as order_created,
		i.uid as invoice_id,
		i."type" as invoice_type,
		i.billingperiodstartdate,
		i.billingperiodenddate,
		i.totalamount,
		case 
			when extract('day' from (i.billingperiodenddate  - i.billingperiodstartdate)) < 1 then 1
			else extract('day' from (i.billingperiodenddate  - i.billingperiodstartdate))
		end as invoice_billing_period,
		i.receiptdate
	from orders o 
	left join invoice i on i.orderid = o.uid 
	where o.productid not in (1246, 67, 68, 81) -- subscribtion plans we do not need
	and i.status = 'paid'
)

select 
	date_trunc('month', "range"."date") as "month",
	sum(invoices_amount.amount_per_day) as mrr,
	count(distinct invoices_amount.account_id) as paying_accs
from 
(
	select 
		generate_series(
			date_trunc('day' , '2021-01-01'::date), -- $__timeFrom()::date for grafana
			date_trunc('day', '2022-09-01'::date), -- $__timeTo()::date for grafana
			'1 day') AS "date"
) "range"

left join
(
	select
		i.account_id,
		i.order_id, 
		i.invoice_id,
		i.billingperiodstartdate,
		i.billingperiodenddate,
		i.invoice_billing_period,
		i.totalamount,
		i.totalamount - coalesce(r.refund_amount, 0) as amount,
		(i.totalamount - coalesce(r.refund_amount, 0)) / invoice_billing_period as amount_per_day
	from annual_self_service_invoices i
	left join refunds r on r.invoice_id = i.invoice_id
) invoices_amount
on "range"."date" between invoices_amount.billingperiodstartdate and invoices_amount.billingperiodenddate
where invoices_amount.amount_per_day > 0
group by date_trunc('month', "range"."date")