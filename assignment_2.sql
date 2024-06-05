-- Droppa il database se esiste già
DROP DATABASE IF EXISTS business_db;

-- Crea database
CREATE DATABASE business_db;

\c business_db

-- Imposta il formato della data
SET datestyle = 'DMY';

-- Variabili per i nomi delle tabelle
DO $$ 
BEGIN
    PERFORM 1 
    FROM information_schema.tables 
    WHERE table_name='variable_table';
    
    IF NOT FOUND THEN
        CREATE TABLE variable_table (
            variable_name TEXT PRIMARY KEY,
            variable_value TEXT
        );
        
        INSERT INTO variable_table (variable_name, variable_value) VALUES
        ('DOMAIN_TABLE', 'domain'),
        ('HOSTING_TABLE', 'hosting'),
        ('INVOICE_TABLE', 'invoice');
    END IF;
END $$;

DO $$
DECLARE
    domain_table TEXT;
    hosting_table TEXT;
    invoice_table TEXT;
    rows_inserted INT;
BEGIN
    SELECT variable_value INTO domain_table FROM variable_table WHERE variable_name = 'DOMAIN_TABLE';
    SELECT variable_value INTO hosting_table FROM variable_table WHERE variable_name = 'HOSTING_TABLE';
    SELECT variable_value INTO invoice_table FROM variable_table WHERE variable_name = 'INVOICE_TABLE';

    -- Droppa la tabella domain se esiste già
    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(domain_table);
    RAISE NOTICE 'Table % has been dropped', domain_table;

    -- Crea la tabella per domain.csv
    EXECUTE '
    CREATE TABLE ' || quote_ident(domain_table) || ' (
        id INT PRIMARY KEY,
        userid INT,
        type VARCHAR(25),
        registrationdate TIMESTAMP,
        domain VARCHAR(250),
        status VARCHAR(20),
        nextduedate TIMESTAMP
    )';
    RAISE NOTICE 'Table % has been created', domain_table;

    -- Importazione dei dati nella tabella domain
    EXECUTE '
    COPY ' || quote_ident(domain_table) || '(id, userid, type, registrationdate, domain, status, nextduedate)
    FROM ''/data/domain.csv''
    DELIMITER '',''
    CSV HEADER';
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    RAISE NOTICE 'Data has been imported into table %: % rows inserted', domain_table, rows_inserted;

    -- Droppa la tabella hosting se esiste già
    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(hosting_table);
    RAISE NOTICE 'Table % has been dropped', hosting_table;

    -- Crea la tabella per hosting.csv
    EXECUTE '
    CREATE TABLE ' || quote_ident(hosting_table) || ' (
        id INT PRIMARY KEY,
        userid INT,
        packageid INT,
        regdate TIMESTAMP,
        domain VARCHAR(250),
        domainstatus VARCHAR(50),
        nextduedate TIMESTAMP
    )';
    RAISE NOTICE 'Table % has been created', hosting_table;

    -- Importazione dei dati nella tabella hosting
    EXECUTE '
    COPY ' || quote_ident(hosting_table) || '(id, userid, packageid, regdate, domain, domainstatus, nextduedate)
    FROM ''/data/hosting.csv''
    DELIMITER '',''
    CSV HEADER';
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    RAISE NOTICE 'Data has been imported into table %: % rows inserted', hosting_table, rows_inserted;

    -- Droppa la tabella invoice se esiste già
    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(invoice_table);
    RAISE NOTICE 'Table % has been dropped', invoice_table;

    -- Crea la tabella per invoice.csv senza chiave primaria
    EXECUTE '
    CREATE TABLE ' || quote_ident(invoice_table) || ' (
        invoiceid INT,
        userid  INT,
        type VARCHAR(50),
        relid INT,
        description VARCHAR(500),
        amount NUMERIC(10, 2),
        duedate TIMESTAMP,
        invoice_label VARCHAR(50)
    )';
    RAISE NOTICE 'Table % has been created', invoice_table;

    -- Importazione dei dati nella tabella invoice
    EXECUTE '
    COPY ' || quote_ident(invoice_table) || '(invoiceid, userid, type, relid, description, amount, duedate, invoice_label)
    FROM ''/data/invoice.csv''
    DELIMITER '',''
    CSV HEADER';
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    RAISE NOTICE 'Data has been imported into table %: % rows inserted', invoice_table, rows_inserted;

END $$;

--- PART 2 ----

DO $$
DECLARE
    invoice_table TEXT;
BEGIN
    SELECT variable_value INTO invoice_table FROM variable_table WHERE variable_name = 'INVOICE_TABLE';

    EXECUTE 'ALTER TABLE ' || quote_ident(invoice_table) || ' ADD COLUMN purchase_date TIMESTAMP';
    RAISE NOTICE 'Column purchase_date has been added to table %', invoice_table;

    EXECUTE '
        UPDATE ' || quote_ident(invoice_table) || ' AS i
        SET purchase_date = sub.min_purchase_date
        FROM (
            SELECT userid, relid, MIN(duedate) AS min_purchase_date
            FROM ' || quote_ident(invoice_table) || '
            GROUP BY userid, relid
        ) AS sub
        WHERE i.userid = sub.userid AND i.relid = sub.relid';
    RAISE NOTICE 'Purchase date has been updated';

    EXECUTE '
        UPDATE ' || quote_ident(invoice_table) || '
        SET invoice_label = CASE
            WHEN purchase_date = duedate THEN ''new''
            ELSE ''renew''
            END';
    RAISE NOTICE 'Invoice label has been updated';

END $$;

--- PART 3: REPORT VIEW Query

SELECT
    i.type,
    SUM(i.amount) AS total_revenue
FROM
    invoice i
LEFT JOIN
    hosting h ON i.relid = h.id
WHERE
    i.duedate = '2019-10-01'
    AND h.domainstatus = 'Active'
GROUP BY
    i.type;
