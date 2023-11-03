@funsql begin

device(match...) = begin
    from(device_exposure)
    $(length(match) == 0 ? @funsql(define()) : @funsql(filter(device_matches($match))))
    left_join(visit => visit_occurrence(),
        visit_occurrence_id == visit_occurrence.visit_occurrence_id, optional = true)
    join(event => begin
        from(device_exposure)
        define(
            table_name => "device_exposure",
            concept_id => device_concept_id,
            end_date => device_exposure_end_date,
            is_historical => device_exposure_id > 1500000000,
            start_date => device_exposure_start_date,
            source_concept_id => device_source_concept_id)
    end, device_exposure_id == event.device_exposure_id, optional = true)
end

device_exposure(match...) = device($match...)
device_matches(match...) = concept_matches($match; match_prefix=device)

end
