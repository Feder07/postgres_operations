\c business_db


SELECT 
	'domain_invoice_relat: relid' as relationship_descr,
    COUNT(*) AS match_count, 
    (SELECT COUNT(*) FROM invoice) AS total_invoices,
    (COUNT(*)::DECIMAL / (SELECT COUNT(*) FROM invoice)) * 100 AS likelihood_percent	
FROM 
    invoice i
JOIN 
    domain d ON i.relid = d.id
	
UNION ALL
SELECT 
	'domain_invoice_relat: invoiceid' as relationship_descr,
    COUNT(*) AS match_count, 
    (SELECT COUNT(*) FROM invoice) AS total_invoices,
    (COUNT(*)::DECIMAL / (SELECT COUNT(*) FROM invoice)) * 100 AS likelihood_percent	
FROM 
    invoice i
JOIN 
    domain d ON i.invoiceid = d.id
	
UNION ALL
	SELECT 
	'domain_hosting_relat: relid' as relationship_descr,
    COUNT(*) AS match_count, 
    (SELECT COUNT(*) FROM invoice) AS total_invoices,
    (COUNT(*)::DECIMAL / (SELECT COUNT(*) FROM invoice)) * 100 AS likelihood_percent
	
FROM 
    invoice i
JOIN 
    hosting h ON i.relid = h.id	
	
UNION ALL
	SELECT 
	'domain_hosting_relat: invoiceid' as relationship_descr,
    COUNT(*) AS match_count, 
    (SELECT COUNT(*) FROM invoice) AS total_invoices,
    (COUNT(*)::DECIMAL / (SELECT COUNT(*) FROM invoice)) * 100 AS likelihood_percent
	
FROM 
    invoice i
JOIN 
    hosting h ON i.invoiceid = h.id	
	
	;


