@funsql begin

device_exposure() = begin
    from(device_exposure)
    define(is_historical => device_exposure_id > 1500000000)
end

end
