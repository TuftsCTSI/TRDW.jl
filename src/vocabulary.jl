format_as_enum(q::FunSQL.SQLNode; descend=false) = @funsql begin
    from(concept)
    filter(is_null(invalid_reason))
    filter(in(standard_concept, "S", "C"))
    $q
    define(item => regexp_replace(concept_name, "[ \\,\\-\\/]+", "_"))
    define(item => ` split(?, ?)[0] `(item, "_\\("))
    $(!descend ? @funsql(define(value => concept_id)) : @funsql(begin
        join(ca => from(concept_ancestor), ca.ancestor_concept_id == concept_id)
        group(concept_id, item)
        define(middle=> array_join(collect_set[ca.descendant_concept_id], ","))
        define(value=> concat("[", middle, "]"))
    end))
    define(item => concat(lower(item), " = ", value))
    order(item)
    select(item)
end

print_as_enum(db::FunSQL.SQLConnection, q::FunSQL.SQLNode; descend=false) =
    print("        " * join(TRDW.run(db, format_as_enum(q; descend)).item, "\n        "))

module Race
    # filter(domain_id == "Race").filter(concept_id<10000)
    @enum T begin
        american_indian_or_alaska_native = 8657
        asian = 8515
        black_or_african_american = 8516
        native_hawaiian_or_other_pacific_islander = 8557
        white = 8527
    end
end

module Ethnicity
    # filter(domain_id == "Ethnicity")
    @enum T begin
        hispanic_or_latino = 38003563
        not_hispanic_or_latino = 38003564
    end
end

module ConditionStatus
    # filter(domain_id == "Condition Status")
    @enum T begin
        admission_diagnosis = 32890
        cause_of_death = 32891
        condition_to_be_diagnosed_by_procedure = 32892
        confirmed_diagnosis = 32893
        contributory_cause_of_death = 32894
        death_diagnosis = 32895
        discharge_diagnosis = 32896
        immediate_cause_of_death = 32897
        postoperative_diagnosis = 32898
        preliminary_diagnosis = 32899
        preoperative_diagnosis = 32900
        primary_admission_diagnosis = 32901
        primary_diagnosis = 32902
        primary_discharge_diagnosis = 32903
        primary_referral_diagnosis = 32904
        referral_diagnosis = 32905
        resolved_condition = 32906
        secondary_admission_diagnosis = 32907
        secondary_diagnosis = 32908
        secondary_discharge_diagnosis = 32909
        secondary_referral_diagnosis = 32910
        underlying_cause_of_death = 32911
    end
end

module Specialty
    # join(p=>from(provider), p.specialty_concept_id == concept_id).
    # join(v=>from(visit_occurrence), p.provider_id == v.provider_id).
    # group(concept_id, concept_name).concept_ancestors()
    @enum T begin
        acute_internal_medicine = 903264
        addiction_medicine = 38004498
        adolescent_medicine = 45756747
        advanced_heart_failure_and_transplant_cardiology = 903279
        allergy = 38003830
        allergy_immunology = 38004448
        allied_health_professional = 32580
        anesthesiology = 38004450
        audiology = 38004489
        behavioral_health_counselor = 38003623
        breast_surgery = 44777667
        cardiac_surgery = 38004497
        cardiology = 38004451
        cardiovascular_disease = 45756754
        case_manager_care_coordinator = 38003781
        certified_respiratory_therapist = 38004090
        child_and_adolescent_psychiatry = 45756756
        chiropractic = 38004475
        clinical_cardiac_electrophysiology = 903274
        clinical_cytogenetics_and_genomics = 32411
        clinical_genetics = 45756760
        clinical_genetics_and_genomics = 32412
        clinical_laboratory = 38004692
        colorectal_surgery = 38004471
        cornea_and_external_ophthalmology = 903243
        counselor = 32578
        critical_care = 38004500
        dentistry = 903277
        dermatology = 38004452
        emergency_medicine = 38004510
        endocrinology = 38004485
        family_medicine = 38003851
        gastroenterology = 38004455
        general_dentistry = 38003675
        general_practice = 38004446
        general_surgery = 38004447
        geriatric_medicine = 38004478
        glaucoma_ophthalmology = 903238
        gynecology = 38003902
        gynecology_oncology = 38004513
        hand_surgery = 38004480
        hematology = 38004501
        hematology_oncology = 38004502
        home_health_aide = 38004436
        hospice_and_palliative_care = 38004462
        infectious_disease = 38004484
        intermediate_care = 44777808
        internal_medicine = 38004456
        interventional_cardiology = 903276
        interventional_radiology = 38004511
        lactation_consultant = 38003802
        mammography_technologist = 38004178
        maternal_and_fetal_medicine = 45756780
        maxillofacial_surgery = 38004504
        medical_oncology = 38004507
        medical_physician_assistant = 38004370
        midwife = 38003807
        neonatal_perinatal_medicine = 45756786
        nephrology = 38004479
        neurocritical_care_medicine = 903250
        neurology = 38004458
        neurosurgery = 38004459
        nuclear_medicine = 38004476
        nurse = 32581
        nurse_practitioner = 38004487
        nutritionist = 38003688
        obesity = 38003864
        obstetrics = 38003905
        obstetrics_gynecology = 38004461
        occupatioal_medicine = 44777713
        occupational_therapy = 38004492
        ophthalmic_plastic_and_reconstructive_surgery = 903244
        ophthalmology = 38004463
        optometry = 38004481
        oral_surgery = 38004464
        orthopedic_surgery = 38004465
        otolaryngology = 38004449
        paediatric_neurology = 44777781
        paediatric_ophthalmology = 44777755
        pain_management = 38004494
        pastoral_behavioral_health_counselor = 38003626
        pathology = 38004466
        pediatric_anesthesiology = 45756804
        pediatric_cardiology = 45756805
        pediatric_emergency_medicine = 45756808
        pediatric_endocrinology = 45756809
        pediatric_gastroenterology = 45756810
        pediatric_hematology_oncology = 45756811
        pediatric_infectious_diseases = 45756812
        pediatric_medicine = 38004477
        pediatric_nephrology = 45756813
        pediatric_pulmonology = 45756815
        pediatric_rheumatology = 45756818
        pediatric_surgery = 45756819
        pediatric_urology = 45756821
        pharmacist = 38003810
        physical_medicine_and_rehabilitation = 38004468
        physical_therapist = 38004490
        physician = 32577
        physician_assistant = 38004512
        physician_diagnostic_radiology = 38004675
        plastic_and_reconstructive_surgery = 38004467
        podiatry = 38004486
        preventive_medicine = 38004503
        psychiatry = 38004469
        psychiatry_or_neurology = 33005
        psychology = 38004488
        pulmonary_disease = 38004472
        radiation_oncology = 38004509
        radiologic_technologist = 38004171
        radiology = 45756825
        registered_ambulatory_care_nurse = 38003755
        registered_dietitian = 38003690
        registered_dietitian_or_nutrition_professional = 38004694
        registered_infusion_therapy_nurse = 38003737
        registered_nurse = 38003716
        registered_wound_care_nurse = 38003762
        retina_ophthalmology = 903239
        rheumatology = 38004491
        sleep_medicine = 903275
        social_worker = 38004499
        speech_language_and_hearing_specialist_technologist = 38004122
        speech_language_assistant = 38004124
        speech_language_pathology = 38004460
        sports_medicine = 903256
        surgical_oncology = 38004508
        thoracic_and_cardiac_surgery = 45756830
        thoracic_surgery = 38004473
        transplant_surgery = 38003827
        trauma_surgery = 38004016
        urology = 38004474
        vascular_surgery = 38004496
    end
end

module DoseFormGroup
    # filter(concept_class_id == "Dose Form Group")
    @enum T begin
        buccal_product = 36244020
        chewable_product = 36244035
        crystal_product = 36244019
        dental_product = 36217215
        disintegrating_oral_product = 36244032
        drug_implant_product = 36217219
        flake_product = 36244036
        granule_product = 36244027
        inhalant_product = 36217207
        injectable_product = 36217210
        intraperitoneal_product = 36217221
        intratracheal_product = 36248213
        irrigation_product = 36217222
        lozenge_product = 36244021
        medicated_pad_or_tape = 36217208
        mouthwash_product = 36244022
        mucosal_product = 36217212
        nasal_product = 36217213
        ophthalmic_product = 36217218
        oral_cream_product = 36244024
        oral_film_product = 37498345
        oral_foam_product = 36244028
        oral_gel_product = 36244041
        oral_liquid_product = 36217220
        oral_ointment_product = 36244023
        oral_paste_product = 36244029
        oral_powder_product = 36244030
        oral_product = 36217214
        oral_spray_product = 36244037
        otic_product = 36217217
        paste_product = 36217223
        pellet_product = 36244033
        pill = 36217216
        prefilled_applicator_product = 36217224
        pudding_product = 36244038
        pyelocalyceal_product = 1146249
        rectal_product = 36217211
        shampoo_product = 36244034
        soap_product = 36244040
        sublingual_product = 36244025
        toothpaste_product = 36244026
        topical_product = 36217206
        transdermal_product = 36244042
        urethral_product = 36217225
        vaginal_product = 36217209
        wafer_product = 36244031
    end
end
