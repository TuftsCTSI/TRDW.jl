module PlutoRunnerExt

using TRDW

function __init__()
    @eval begin
        Main.PlutoRunner.pluto_showable(::MIME"application/vnd.pluto.table+object", ::TRDW.SQLResult) =
            false
    end
end

end
