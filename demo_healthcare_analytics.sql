
    -- Healthcare Patient Journey Analysis
    WITH patient_cohorts AS (
        SELECT 
            patient_id,
            admission_date,
            discharge_date,
            primary_diagnosis,
            age_group,
            insurance_type,
            DATEDIFF(day, admission_date, discharge_date) as length_of_stay
        FROM patient_admissions
        WHERE admission_date >= '2023-01-01'
    ),
    
    treatment_pathways AS (
        SELECT 
            pc.patient_id,
            pc.primary_diagnosis,
            pc.length_of_stay,
            t.treatment_type,
            t.treatment_date,
            t.cost,
            ROW_NUMBER() OVER (
                PARTITION BY pc.patient_id 
                ORDER BY t.treatment_date
            ) as treatment_sequence
        FROM patient_cohorts pc
        INNER JOIN treatments t ON pc.patient_id = t.patient_id
        WHERE t.treatment_date BETWEEN pc.admission_date AND pc.discharge_date
    ),
    
    readmission_analysis AS (
        SELECT 
            tp.patient_id,
            tp.primary_diagnosis,
            COUNT(DISTINCT tp.treatment_type) as treatment_variety,
            SUM(tp.cost) as total_treatment_cost,
            -- Check for readmissions within 30 days
            CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM patient_admissions pa2 
                    WHERE pa2.patient_id = tp.patient_id 
                    AND pa2.admission_date > (
                        SELECT MAX(pa1.discharge_date) 
                        FROM patient_admissions pa1 
                        WHERE pa1.patient_id = tp.patient_id
                    )
                    AND DATEDIFF(day, (
                        SELECT MAX(pa1.discharge_date) 
                        FROM patient_admissions pa1 
                        WHERE pa1.patient_id = tp.patient_id
                    ), pa2.admission_date) <= 30
                ) THEN 1 
                ELSE 0 
            END as readmitted_30_days
        FROM treatment_pathways tp
        GROUP BY tp.patient_id, tp.primary_diagnosis
    )
    
    SELECT 
        ra.primary_diagnosis,
        COUNT(DISTINCT ra.patient_id) as patient_count,
        AVG(ra.treatment_variety) as avg_treatments_per_patient,
        AVG(ra.total_treatment_cost) as avg_cost_per_patient,
        SUM(ra.readmitted_30_days) as readmission_count,
        ROUND(
            SUM(ra.readmitted_30_days) * 100.0 / COUNT(DISTINCT ra.patient_id), 2
        ) as readmission_rate_pct,
        outcome_metrics.avg_recovery_days,
        cost_comparison.national_avg_cost
    FROM readmission_analysis ra
    LEFT JOIN (
        -- Recovery time analysis
        SELECT 
            primary_diagnosis,
            AVG(length_of_stay) as avg_recovery_days
        FROM patient_cohorts
        GROUP BY primary_diagnosis
    ) outcome_metrics ON ra.primary_diagnosis = outcome_metrics.primary_diagnosis
    LEFT JOIN (
        -- Cost benchmarking
        SELECT 
            diagnosis_code,
            avg_national_cost as national_avg_cost
        FROM national_cost_benchmarks
    ) cost_comparison ON ra.primary_diagnosis = cost_comparison.diagnosis_code
    GROUP BY ra.primary_diagnosis, outcome_metrics.avg_recovery_days, cost_comparison.national_avg_cost
    HAVING COUNT(DISTINCT ra.patient_id) >= 10  -- Only diagnoses with sufficient volume
    ORDER BY readmission_rate_pct DESC;
    