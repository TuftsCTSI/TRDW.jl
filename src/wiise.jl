"""
    @funsql trdw_to_wiise()

Correlate `person` records with WIISE identifiers.

The input dataset must contain column `person_id`.

The output preserves `person_id` from the input dataset and adds columns `wiise_id` and `system_name` that identify the record in the WIISE Viewer.
"""
@funsql trdw_to_wiise() = begin
    as(person)
    over(
        append(
            begin
                person => from(person)
                left_join(
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
                    coalesce(epicpatientid_omoppersonid_map.related_person_id, person.person_id) == omop_common_person_map.person_id)
                join(
                    wiise_patient => begin
                        from(`wiise.patient`)
                        define(source => meta >> source)
                        filter(source == "tuftssoarian")
                        define(legacy_mrn => fun(`filter(?, i -> i.system = ?)[0].value`, identifier, "2.16.840.1.113883.3.650.387"))
                    end,
                    omop_common_person_map.mrn == wiise_patient.legacy_mrn)
            end
#
# WIISE Archive Viewer doesn't support "epic" links... currently.
#
#           ,
#           begin
#               person => from(person)
#               join(
#                   person_map => from(`person_map.person_map`),
#                   person.person_id == person_map.person_id)
#               join(
#                   wiise_patient => begin
#                       from(`global.patient`)
#                       group(id, system_epic_id)
#                       define(source => "epic")
#                   end,
#                   person_map.person_source_value == wiise_patient.system_epic_id)
#           end
#
           )
       )
    with(
        `trdwlegacyred.epicpatientid_omoppersonid_map` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdwlegacyred], name = :epicpatientid_omoppersonid_map, columns = [:person_id, :EpicPatientId]))),
        `trdwlegacysoarian.omop_common_person_map` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdwlegacysoarian], name = :omop_common_person_map, columns = [:person_id, :mrn]))),
        `person_map.person_map` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, :person_map], name = :person_map, columns = [:person_id, :person_source_value]))),
        `wiise.patient` =>
            from($(FunSQL.SQLTable(qualifiers = [:main, :wiise], name = :patient, columns = [:id, :meta, :identifier]))),
        `global.patient` =>
            from($(FunSQL.SQLTable(qualifiers = [:main, :global], name = :patient, columns = [:id, :system_epic_id]))))
    define(
        person.person_id,
        wiise_id => wiise_patient.id,
        system_name => wiise_patient.source)
end

"""
    @funsql join_wiise_id(html = false)

Add a column `wiise_id` containing a space-separated list of WIISE patient identifiers.

When `html` is `true`, the column is called `wiise_id_html` and is rendered as a link to the WIISE Viewer.

The input dataset is expected to contain column `person_id`.
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
                    define(
                        wiise_id_text => concat(system_name, ":", wiise_id),
                        wiise_id_html => wiise_id_to_html(wiise_id, system_name))
                    define(`struct`(system_name, wiise_id, $(html ? :wiise_id_html : :wiise_id_text)))
                    group(person_id)
                end,
                person_id == trdw_to_wiise.person_id)
            define(
                $(html ? :wiise_id_html : :wiise_id) =>
                    array_join(fun(`?.col3`, array_sort(trdw_to_wiise.collect_set(`struct`))), " "))
        end)
end

"""
    @funsql wiise_id_to_html(wiise_id, system_name)

Convert a WIISE patient identifier to a WIISE Viewer link.
"""
@funsql wiise_id_to_html(wiise_id, system_name) =
    case(
        fun(`(? RLIKE ?)`, $wiise_id, "^[0-9A-Fa-f-]+\$") && fun(`(? RLIKE ?)`, $system_name, "^[a-z]+\$"),
        concat(
            """<a href="https://wellforce.muspell.314ecorp.com/patient-info/""",
            $wiise_id,
            "?system_name=",
            $system_name,
            """\">""",
            concat($system_name, ":", substr($wiise_id, 1, 8)),
            """</a>"""))
