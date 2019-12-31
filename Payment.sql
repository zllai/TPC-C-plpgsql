CREATE FUNCTION payment(w_id smallint, 
                        d_id tinyint,
                        h_amount float,
                        c_w_id smallint,
                        c_d_id tinyint,
                        c_id integer,
                        c_last varchar(32),
                        h_date timestamp)
                        RETURNS RECORD AS
$$
DECLARE
    getWarehouse text := 'SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = $1';
    updateWarehouseBalance text := 'UPDATE WAREHOUSE SET W_YTD = W_YTD + $1 WHERE W_ID = $2';
    getDistrict text := 'SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = $1 AND D_ID = $2';
    updateDistrictBalance text := 'UPDATE DISTRICT SET D_YTD = D_YTD + $1 WHERE D_W_ID  = $2 AND D_ID = $3';
    getCustomerByCustomerId text := 'SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_ID = $3';
    getCustomersByLastName text := 'SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_LAST = $3 ORDER BY C_FIRST';
    updateBCCustomer text := 'UPDATE CUSTOMER SET C_BALANCE = $1, C_YTD_PAYMENT = $2, C_PAYMENT_CNT = $3, C_DATA = $4 WHERE C_W_ID = $5 AND C_D_ID = $6 AND C_ID = $7';
    updateGCCustomer text := 'UPDATE CUSTOMER SET C_BALANCE = $1, C_YTD_PAYMENT = $2, C_PAYMENT_CNT = $3 WHERE C_W_ID = $4 AND C_D_ID = $5 AND C_ID = $6';
    insertHistory text := 'INSERT INTO HISTORY VALUES ($1, $2, $3, $4, $5, $6, $7, $8)';
    max_c_data CONSTANT integer := 500;
    customer RECORD;
    all_customer RECORD;
    c_balance float;
    c_ytd_payment float;
    c_payment_cnt integer;
    c_data text;
    warehouse RECORD;
    district RECORD;
    new_data text;
    h_data text;
BEGIN
    IF c_id != NULL THEN
        EXECUTE getCustomerByCustomerId INTO customer USING w_id, d_id, c_id;
    ELSE
        EXECUTE getCustomersByLastName INTO all_customer USING w_id, d_id, c_last;
        ASSERT all_customer != NULL;
        customer = all_customer[(array_length(all_customer) - 1) / 2];
    END IF;
    ASSERT customer != NULL;
    c_id = customer.C_ID;
    c_balance = customer.C_BALANCE - h_amount;
    c_ytd_payment = customer.C_YTD_PAYMENT + h_amount;
    c_payment_cnt = customer.C_PAYMENT_CNT + 1;
    c_data = customer.C_DATA;

    EXECUTE getWarehouse INTO warehouse USING w_id;
    EXECUTE getDistrict INTO district USING d_id;
    EXECUTE updateWarehouseBalance USING h_amount, w_id;
    EXECUTE updateDistrictBalance USING h_amount, w_id, d_id;

    IF customer.C_CREDIT = 'BC' THEN
        newData = FORMAT('%s %s %s %s %s %s', c_id, c_d_id, c_w_id, d_id, w_id, h_amount);
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