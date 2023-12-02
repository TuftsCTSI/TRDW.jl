"""
    @funsql trdw_to_wiise()

Correlate `person` records with WIISE identifiers.

The input dataset must contain columns `person_id` and `person_source_value`.

The output preserves `person_id` from the input dataset and adds column `wiise_id`, which identifies the record in the table `wiise.patient` and in the WIISE Viewer.
"""
@funsql trdw_to_wiise() = begin
    as(person)
    over(
        append(
            begin
                person => from(person)
                join(
                    epicpatientid_omoppersonid_map => begin
                        from(map)
                        join(related => from(map), EpicPatientId == related.EpicPatientId)
                        with(map => begin
                            from(`trdwlegacyred.epicpatientid_omoppersonid_map`)
                            filter(is_not_null(person_id))
                            group(person_id, EpicPatientId)
                            partition(person_id)
                            filter(count() <= 1)
                        end)
                        group(person_id, related_person_id => related.person_id)
                    end,
                    person.person_id == epicpatientid_omoppersonid_map.person_id)
                join(
                    omop_common_person_map => from(`trdwlegacysoarian.omop_common_person_map`),
                    epicpatientid_omoppersonid_map.related_person_id == omop_common_person_map.person_id)
                join(
                    wiise_patient => begin
                        from(`wiise.patient`)
                        filter(meta >> source == "tuftssoarian")
                        define(legacy_mrn => fun(`filter(?, i -> i.system = ?)[0].value`, identifier, "2.16.840.1.113883.3.650.387"))
                    end,
                    omop_common_person_map.mrn == wiise_patient.legacy_mrn)
            end,
            begin
                person => from(person)
                join(
                    wiise_patient => begin
                        from(`wiise.patient`)
                        filter(meta >> source == "tuftsmedicineclarity")
                        define(pat_id => fun(`filter(?, i -> i.system = ?)[0].value`, identifier, "EpicWFPatientEPICId"))
                    end,
                    person.person_source_value == wiise_patient.pat_id)
            end))
    with(
        `trdwlegacyred.epicpatientid_omoppersonid_map` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdwlegacyred], name = :epicpatientid_omoppersonid_map, columns = [:person_id, :EpicPatientId]))),
        `trdwlegacysoarian.omop_common_person_map` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdwlegacysoarian], name = :omop_common_person_map, columns = [:person_id, :mrn]))),
        `wiise.patient` =>
            from($(FunSQL.SQLTable(qualifiers = [:main, :wiise], name = :patient, columns = [:id, :meta, :identifier]))))
    define(
        person.person_id,
        wiise_id => wiise_patient.id)
end

"""
    @funsql join_wiise_id(html = false)

Add a column `wiise_id` containing a space-separated list of WIISE patient identifiers.

When `html` is `true`, the column is called `wiise_id_html` and is rendered as a link to the WIISE Viewer.

The input dataset is expected to contain columns `person_id` and `person_source_value`.
"""
@funsql join_wiise_id(; html = false) = begin
    as(person)
    over(
        begin
            from(person)
            left_join(
                trdw_to_wiise => begin
                    from(person)
                    trdw_to_wiise()
                    define(wiise_id_html => wiise_id_to_html(wiise_id))
                    group(person_id)
                end,
                person_id == trdw_to_wiise.person_id)
            define(array_join(trdw_to_wiise.collect_set($(html ? :wiise_id_html : :wiise_id)), " ").as($(html ? :wiise_id_html : :wiise_id)))
        end)
end

"""
    @funsql wiise_id_to_html(str)

Convert a WIISE patient identifier to a WIISE Viewer link.
"""
@funsql wiise_id_to_html(str) =
    case(
        fun(`(? RLIKE ?)`, $str, "^[0-9A-Fa-f-]+\$"),
        concat(
            """<a href="https://wellforce.muspell.314ecorp.com/patient-info/""",
            $str,
            """\">""",
            substr($str, 1, 8),
            """</a>"""))
