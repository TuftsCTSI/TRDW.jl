export Excalidraw

struct Excalidraw
    filename::String
    editable::Bool

    Excalidraw(filename; editable = !("CI" in keys(ENV))) =
        new(filename, editable)
end

function Base.show(io::IO, m::MIME"text/html", excalidraw::Excalidraw)
    svg = ""
    if isfile(excalidraw.filename)
        svg = read(excalidraw.filename, String)
    end
    if !excalidraw.editable
        return print(io, """<div><figure>$svg</figure></div>""")
    end
    show(io, m, @htl(
        """
        <div style="position: relative">
        <figure>$(HTML(svg))</figure>
        <script src="https://www.unpkg.com/react@18.3.1/umd/react.development.js"></script>
        <script src="https://www.unpkg.com/react-dom@18.3.1/umd/react-dom.development.js"></script>
        <script src="https://unpkg.com/@excalidraw/excalidraw@0.17.6/dist/excalidraw.development.js"></script>
        <script>
        const save = $(AbstractPlutoDingetjes.Display.with_js_link((svg) -> write(excalidraw.filename, svg)))
        const cellNode = currentScript.closest("pluto-cell")
        const figureNode = currentScript.parentElement.querySelector("figure")
        const appNode = document.createElement("div")
        const initialAppState = ExcalidrawLib.restoreAppState({
            currentItemFontFamily: 2,
            currentItemRoughness: 0,
            exportEmbedScene: true,
            gridModeEnabled: true,
            zoom: 0.5,
        })
        const loadScene = async (svg) => {
            if (svg) {
                const serializer = new XMLSerializer()
                const blob = new Blob([serializer.serializeToString(svg)], { type: "image/svg+xml" })
                const scene = await ExcalidrawLib.loadFromBlob(blob, null, null)
                return { elements: scene.elements, appState: initialAppState, files: scene.files }
            }
            else {
                return { appState: initialAppState }
            }
        }
        const initialData = await loadScene(figureNode.firstChild)
        const App = () => {
            const [isEditing, setIsEditing] = React.useState(false)
            const [excalidrawAPI, setExcalidrawAPI] = React.useState(null)
            const discardChanges = () => {
                setIsEditing(false)
                setExcalidrawAPI(null)
            }
            const applyChanges = async () => {
                if (excalidrawAPI) {
                    const elements = excalidrawAPI.getSceneElements()
                    if (elements && elements.length) {
                        const svg = await ExcalidrawLib.exportToSvg({
                            elements,
                            appState: initialAppState,
                            exportPadding: 10,
                            files: excalidrawAPI.getFiles(),
                        })
                        const w = parseFloat(svg.getAttribute("width"))
                        const h = parseFloat(svg.getAttribute("height"))
                        if (!isNaN(w) && !isNaN(h)) {
                            svg.setAttribute("width", w/2)
                            svg.setAttribute("height", h/2)
                        }
                        const serializer = new XMLSerializer()
                        await save(serializer.serializeToString(svg))
                        figureNode.replaceChildren(svg)
                        if (cellNode) {
                            cellNode._internal_pluto_actions.set_and_run_multiple([cellNode.getAttribute("id")])
                        }
                    }
                }
                discardChanges()
            }
            return (
                isEditing ?
                    React.createElement(
                        React.Fragment,
                        null,
                        React.createElement(
                            "div",
                            { style: { position: "fixed", left: 0, top: 0, width: "100%", height: "100%", zIndex: 10000 } },
                            React.createElement(
                                ExcalidrawLib.Excalidraw,
                                {
                                    excalidrawAPI: setExcalidrawAPI,
                                    gridModeEnabled: true,
                                    initialData,
                                },
                                React.createElement(
                                    ExcalidrawLib.MainMenu,
                                    null,
                                    React.createElement(
                                        ExcalidrawLib.MainMenu.Item,
                                        { onSelect: applyChanges },
                                        "Apply changes"),
                                    React.createElement(
                                        ExcalidrawLib.MainMenu.Item,
                                        { onSelect: discardChanges },
                                        "Discard changes"))))) :
                    React.createElement(
                        "button",
                        {
                            style: figureNode.firstChild ? { position: "absolute", left: 0, bottom: 0 } : {},
                            onClick: () => setIsEditing(true),
                        },
                        figureNode.firstChild ? "Edit" : "Create new diagram")
            )
        }
        const root = ReactDOM.createRoot(appNode)
        root.render(React.createElement(App))
        return appNode
        </script>
        </div>
        """
    ))
end
