@funsql begin

hiv_concepts() = begin
    append(
        begin
            concept()
            filter(like_acronym(concept_name,
                "HIV",
                "human immunodeficiency virus",
                "immunodeficiency syndrome"))
        end,
        begin
            concept(SNOMED(365866002, "Finding of HIV status"),
                    SNOMED(444356002, "Exposure to Human immunodeficiency virus"))
            concept_descendants()
        end,
        begin
            concept(
                    NDFRT("N0000000127","HIV Integrase Inhibitors"),
                    NDFRT("N0000000246","HIV Protease Inhibitors"),
                    NDFRT("N0000009947","Nucleoside Reverse Transcriptase Inhibitors"),
                    NDFRT("N0000009948","Non-Nucleoside Reverse Transcriptase Inhibitors"),
                    NDFRT("N0000181002","HIV Fusion Protein Inhibitors"))
            concept_relatives("MoA of")
            concept_relatives("NDFRT - RxNorm eq")
            concept_descendants()
        end)
    deduplicate(concept_id)
end

filter_hiv_concepts(concept_id) = begin
    left_join(hiv_concept => hiv_concepts(), $concept_id == hiv_concept.concept_id)
    filter(is_null(hiv_concept.concept_id))
end

end
