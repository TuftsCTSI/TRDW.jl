@funsql begin

link_patient_soarian_to_trdw(PatientOID=nothing) = begin
    left_join(mir_sc_patientidentifiers => begin
                  from(`trdwlegacysoarian.mir_sc_patientidentifiers`)
                  filter(Type == "MR" && IsDeleted != 1)
        end, mir_sc_patientidentifiers.Patient_oid == $(something(PatientOID, :PatientOID)))
    left_join(omop_common_person_map => from(`trdwlegacysoarian.omop_common_person_map`),
              omop_common_person_map.mrn == mir_sc_patientidentifiers.Value)
    with(
        `trdwlegacysoarian.omop_common_person_map` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdwlegacysoarian], name = :omop_common_person_map,
                                   columns = [:person_id, :mrn]))),
        `trdwlegacysoarian.mir_sc_patientidentifiers` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdwlegacysoarian], name = :mir_sc_patientidentifiers,
                                   columns = [:Type, :IsDeleted, :Patient_oid, :Value]))))
    define(omop_common_person_map.person_id)
end

link_patient_trdw_to_soarian(person_id=nothing) = begin
    left_join(omop_common_person_map => from(`trdwlegacysoarian.omop_common_person_map`),
              omop_common_person_map.person_id == $(something(person_id,:person_id)))
    left_join(mir_sc_patientidentifiers => begin
                  from(`trdwlegacysoarian.mir_sc_patientidentifiers`)
                  filter(Type == "MR" && IsDeleted != 1)
        end, omop_common_person_map.mrn == mir_sc_patientidentifiers.Value)
    with(
        `trdwlegacysoarian.omop_common_person_map` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdwlegacysoarian], name = :omop_common_person_map,
                                   columns = [:person_id, :mrn]))),
        `trdwlegacysoarian.mir_sc_patientidentifiers` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdwlegacysoarian], name = :mir_sc_patientidentifiers,
                                   columns = [:Type, :IsDeleted, :Patient_oid, :Value]))))
    define(PatientOID => mir_sc_patientidentifiers.Patient_oid)
end

end
