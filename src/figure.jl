# Wrap a plot in a <figure> node with caption.

export figure

figcaption(::Nothing) =
	""

figcaption(caption) =
    @htl """<figcaption>$caption</figcaption>"""

figure(args...; caption = nothing, kws...) =
    @htl """<figure>$(figcaption(caption))$(figcontent(args...; kws...))</figure>"""

# Can be defined in extensions.
function figcontent
end
