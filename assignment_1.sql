-- Droppa il database se esiste già
DROP DATABASE IF EXISTS currency_db;

-- creo database
CREATE DATABASE currency_db;

\c currency_db

-- Droppa la tabella currency_d se esiste già
DROP TABLE IF EXISTS currency_d;

CREATE TABLE currency_d (
currency_key INT PRIMARY KEY NOT NULL, 
currency_code bpchar(3), 
currency_name VARCHAR(200) , 
currency_symbol bpchar(4) );


-- Droppa la tabella currency_conversion_f se esiste già
DROP TABLE IF EXISTS currency_conversion_f;

CREATE TABLE currency_conversion_f (
    conversion_date DATE NOT NULL,
    source_currency_key INT NOT NULL,
    destination_currency_key INT NOT NULL,
    source_destination_exchgrate NUMERIC,
    destination_source_exchgrate NUMERIC,
    source_destination_month_avg NUMERIC,
    destination_source_month_avg NUMERIC,
    source_destination_year_avg NUMERIC,
    destination_source_year_avg NUMERIC,
    exchgrates_source VARCHAR(255),
    PRIMARY KEY (conversion_date, source_currency_key, destination_currency_key),
    FOREIGN KEY (source_currency_key) REFERENCES currency_d(currency_key)
);


-- Caricamento dei dati dal file CSV nella tabella currency_d
COPY currency_d(
	currency_key, 
	currency_code, 
	currency_name, 
	currency_symbol
	)
FROM '/data/currency_d.csv'
DELIMITER ','
CSV HEADER;


-- Caricamento dei dati dal file CSV nella tabella currency_conversion_f
COPY currency_conversion_f(
	conversion_date, 
	source_currency_key, 
	destination_currency_key, 
	source_destination_exchgrate, 
	destination_source_exchgrate, 
	source_destination_month_avg, 
	destination_source_month_avg, 
	source_destination_year_avg, 
	destination_source_year_avg, 
	exchgrates_source
	)
FROM '/data/currency_conversion_f.csv'
DELIMITER ','
CSV HEADER;


CREATE OR REPLACE FUNCTION save_exchange_rate(
    p_conversion_date DATE,                       -- Parametro per la data di conversione
    p_source_currency_key INT,                    -- Parametro per la chiave della valuta di origine
    p_destination_currency_key INT,               -- Parametro per la chiave della valuta di destinazione
    p_source_destination_exchgrate NUMERIC,       -- Parametro per il tasso di cambio da valuta di origine a valuta di destinazione
    p_exchgrates_source VARCHAR(255)              -- Parametro per la fonte del tasso di cambio
) RETURNS VOID AS $$                              -- La funzione restituisce VOID (nessun valore)
DECLARE
    v_destination_source_exchgrate NUMERIC;       -- Dichiarazione di una variabile per il tasso di cambio inverso

BEGIN
    -- Calcolo del tasso inverso
    v_destination_source_exchgrate := 1 / p_source_destination_exchgrate;

    -- Inserimento del tasso di cambio diretto
    INSERT INTO currency_conversion_f (
        conversion_date, 
        source_currency_key, 
        destination_currency_key, 
        source_destination_exchgrate, 
        destination_source_exchgrate, 
        exchgrates_source
    )
    VALUES (
        p_conversion_date, 
        p_source_currency_key, 
        p_destination_currency_key, 
        p_source_destination_exchgrate, 
        v_destination_source_exchgrate, 
        p_exchgrates_source
    );

    -- Inserimento del tasso di cambio inverso
    INSERT INTO currency_conversion_f (
        conversion_date, 
        source_currency_key, 
        destination_currency_key, 
        source_destination_exchgrate, 
        destination_source_exchgrate, 
        exchgrates_source
    )
    VALUES (
        p_conversion_date, 
        p_destination_currency_key, 
        p_source_currency_key, 
        v_destination_source_exchgrate, 
        p_source_destination_exchgrate, 
        p_exchgrates_source
    );
END;

$$ LANGUAGE plpgsql;


-- Funzione per convertire un importo da una valuta a un'altra
CREATE OR REPLACE FUNCTION convert_amount(
    p_amount NUMERIC,
    p_date DATE,
    p_currency1_key INT,
    p_currency2_key INT
) RETURNS NUMERIC AS $$
DECLARE
    v_exchange_rate NUMERIC;
BEGIN
    -- Recupera il tasso di cambio più vicino alla data specificata
    SELECT source_destination_exchgrate INTO v_exchange_rate
    FROM currency_conversion_f
    WHERE (source_currency_key = p_currency1_key AND destination_currency_key = p_currency2_key)
       OR (source_currency_key = p_currency2_key AND destination_currency_key = p_currency1_key)
    ORDER BY ABS(EXTRACT(day FROM MAKE_INTERVAL(0, 0, 0, (conversion_date - p_date))::interval))
    LIMIT 1;

    -- Converte l'importo utilizzando il tasso di cambio recuperato
    RETURN p_amount * v_exchange_rate;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_averages() RETURNS VOID AS $$

BEGIN
    -- Aggiornamento della media mensile
    UPDATE currency_conversion_f
    SET source_destination_month_avg = subquery.avg_rate
    FROM (
        SELECT source_currency_key, destination_currency_key, AVG(source_destination_exchgrate) AS avg_rate
        FROM currency_conversion_f
        WHERE DATE_TRUNC('month', conversion_date) = DATE_TRUNC('month', NOW())
        GROUP BY source_currency_key, destination_currency_key
    ) AS subquery
    WHERE currency_conversion_f.source_currency_key = subquery.source_currency_key
      AND currency_conversion_f.destination_currency_key = subquery.destination_currency_key;

    -- Aggiornamento della media annuale
    UPDATE currency_conversion_f
    SET source_destination_year_avg = subquery.avg_rate
    FROM (
        SELECT source_currency_key, destination_currency_key, AVG(source_destination_exchgrate) AS avg_rate
        FROM currency_conversion_f
        WHERE DATE_TRUNC('year', conversion_date) = DATE_TRUNC('year', NOW())
        GROUP BY source_currency_key, destination_currency_key
    ) AS subquery
    WHERE currency_conversion_f.source_currency_key = subquery.source_currency_key
      AND currency_conversion_f.destination_currency_key = subquery.destination_currency_key;

    -- Restituisci VOID poiché la funzione esegue solo azioni di aggiornamento
    RETURN;
END;
$$ LANGUAGE plpgsql;


-- Esempi di esecuzione delle funzioni
SELECT save_exchange_rate('2024-05-28', 26, 72, 1.15, 'Source 1');
SELECT save_exchange_rate('2024-05-28', 26, 28, 0.85, 'Source 2');

SELECT convert_amount(100, '2024-05-28', 26, 72);
SELECT convert_amount(200, '2024-05-28', 26, 28);

SELECT update_averages();