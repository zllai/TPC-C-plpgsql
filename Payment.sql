DROP TYPE IF EXISTS t_payment_customer CASCADE;
CREATE TYPE t_payment_customer AS (C_ID integer, C_FIRST varchar(32), C_MIDDLE varchar(32), C_LAST varchar(32), C_STREET_1 varchar(32), C_STREET_2 varchar(32), C_CITY varchar(32), C_STATE varchar(2), C_ZIP varchar(9), C_PHONE varchar(32), C_SINCE timestamp, C_CREDIT varchar(2), C_CREDIT_LIM float, C_DISCOUNT float, C_BALANCE float, C_YTD_PAYMENT float, C_PAYMENT_CNT integer, C_DATA varchar(500));
DROP TYPE IF EXISTS t_payment_warehouse CASCADE;
CREATE TYPE t_payment_warehouse AS (W_NAME varchar(16), W_STREET_1 varchar(32), W_STREET_2 varchar(32), W_CITY varchar(32), W_STATE varchar(2), W_ZIP varchar(9));
DROP TYPE IF EXISTS t_payment_district CASCADE;
CREATE TYPE t_payment_district AS (D_NAME varchar(16), D_STREET_1 varchar(32), D_STREET_2 varchar(32), D_CITY varchar(32), D_STATE varchar(2), D_ZIP varchar(9));
DROP TYPE IF EXISTS t_payment_res CASCADE;
CREATE TYPE t_payment_res AS (warehouse t_payment_warehouse, district t_payment_district, customer t_payment_customer);

CREATE OR REPLACE FUNCTION payment(w_id smallint, 
                                   d_id int8,
                                   h_amount float,
                                   c_w_id smallint,
                                   c_d_id int8,
                                   c_id integer,
                                   c_last varchar(32),
                                   h_date timestamp)
                                   RETURNS t_payment_res AS
$$
DECLARE
    getWarehouse text := 'SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = $1';
    updateWarehouseBalance text := 'UPDATE WAREHOUSE SET W_YTD = W_YTD + $1 WHERE W_ID = $2';
    getDistrict text := 'SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = $1 AND D_ID = $2';
    updateDistrictBalance text := 'UPDATE DISTRICT SET D_YTD = D_YTD + $1 WHERE D_W_ID  = $2 AND D_ID = $3';
    getCustomerByCustomerId text := 'SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_ID = $3';
    getCustomersByLastName text := 'SELECT ARRAY(SELECT (C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA) FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_LAST = $3 ORDER BY C_FIRST)';
    updateBCCustomer text := 'UPDATE CUSTOMER SET C_BALANCE = $1, C_YTD_PAYMENT = $2, C_PAYMENT_CNT = $3, C_DATA = $4 WHERE C_W_ID = $5 AND C_D_ID = $6 AND C_ID = $7';
    updateGCCustomer text := 'UPDATE CUSTOMER SET C_BALANCE = $1, C_YTD_PAYMENT = $2, C_PAYMENT_CNT = $3 WHERE C_W_ID = $4 AND C_D_ID = $5 AND C_ID = $6';
    insertHistory text := 'INSERT INTO HISTORY VALUES ($1, $2, $3, $4, $5, $6, $7, $8)';
    max_c_data CONSTANT integer := 500;
    customer t_payment_customer;
    all_customer t_payment_customer[];
    c_balance float;
    c_ytd_payment float;
    c_payment_cnt integer;
    c_data text;
    warehouse t_payment_warehouse;
    district t_payment_district;
    new_data text;
    h_data text;
BEGIN
    IF c_id IS NOT NULL THEN
        EXECUTE getCustomerByCustomerId INTO customer USING w_id, d_id, c_id;
    ELSE
        EXECUTE getCustomersByLastName INTO all_customer USING w_id, d_id, c_last;
        ASSERT all_customer IS NOT NULL;
        customer = all_customer[(array_length(all_customer, 1) - 1) / 2 + 1];
    END IF;
    ASSERT customer IS NOT NULL;
    c_id = customer.C_ID;
    c_balance = customer.C_BALANCE - h_amount;
    c_ytd_payment = customer.C_YTD_PAYMENT + h_amount;
    c_payment_cnt = customer.C_PAYMENT_CNT + 1;
    c_data = customer.C_DATA;

    EXECUTE getWarehouse INTO warehouse USING w_id;
    EXECUTE getDistrict INTO district USING w_id, d_id;
    EXECUTE updateWarehouseBalance USING h_amount, w_id;
    EXECUTE updateDistrictBalance USING h_amount, w_id, d_id;

    IF customer.C_CREDIT = 'BC' THEN
        new_data = FORMAT('%s %s %s %s %s %s', c_id, c_d_id, c_w_id, d_id, w_id, h_amount);
        c_data = new_data || '|' || c_data;
        IF char_length(c_data) > max_c_data THEN
            c_data = substring(c_data from 1 for max_c_data);
        END IF;
        EXECUTE updateBCCustomer USING c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id;
    ELSE
        c_data = '';
        EXECUTE updateGCCustomer USING c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id; 
    END IF;
    h_data = FORMAT('%s    %s', warehouse.W_NAME, district.D_NAME);
    EXECUTE insertHistory USING c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data;
    RETURN ROW(warehouse, district, customer);
END;
$$ LANGUAGE plpgsql;