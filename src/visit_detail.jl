@funsql begin

visit_detail() = begin
    from(visit_detail)
    define(is_historical => visit_detail_id > 1000000000)
end

end
