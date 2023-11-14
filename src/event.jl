@funsql begin

first_event() = begin
    partition(person_id; order_by = [event.start_datetime], name="first_event")
    filter(first_event.row_number() <= 1)
end

event_length() = datediff_day(event.start_datetime, event.end_datetime) + 1

end
