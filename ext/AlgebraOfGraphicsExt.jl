module AlgebraOfGraphicsExt

using AlgebraOfGraphics, TRDW

TRDW.figcontent(args...; kws...) =
    AlgebraOfGraphics.draw(args...; kws...)

function __init__()
    AlgebraOfGraphics.set_aog_theme!()
end

end
