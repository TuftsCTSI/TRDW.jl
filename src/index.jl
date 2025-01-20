# Utilities for the Index notebook.

struct NotebookList
end

function Base.show(io::IO, mime::MIME"text/html", ::NotebookList)

    data = """
    {
      disabled: !window.pluto_disable_ui,
      busy: false,
      notebooks: [],
      latestError: null,

      init() {
        if (!this.disabled) {
          this.refreshNotebooks()
        }
      },

      hasNotebooks() {
        return this.notebooks.length > 0
      },

      refreshNotebooks() {
        this.busy = true
        let p = Promise.resolve(null)
        p = p.then(() => fetch("pluto_export.json"))
        p = p.then((response) => response.ok ? response.json() : Promise.reject(Error(response.statusText)))
        p = p.then((json) => this.notebooks = this.parsePlutoExportJson(json))
        p = p.catch((e) => this.latestError = e)
        p = p.finally(() => this.busy = false)
        return p
      },

      parsePlutoExportJson(json) {
        ns = Object.values(json.notebooks).filter((n) => n.id != "index.jl")
        ns.sort((a, b) => {
          const a_order = Number(a.frontmatter?.order)
          const b_order = Number(b.frontmatter?.order)
          if (isNaN(a_order) && isNaN(b_order)) {
            return a.id < b.id ? -1 : a.id > b.id ? 1 : 0
          }
          else if (isNaN(a_order)) {
            return 1
          }
          else if (isNaN(b_order)) {
            return -1
          }
          else {
            return a_order - b_order
          }
        })
        return ns
      },
    }
    """

    show(
        io,
        mime,
        @htl """
        <div>
          <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.14.8/dist/cdn.min.js" integrity="sha384-X9kJyAubVxnP0hcA+AMMs21U445qsnqhnUF8EBlEpP3a42Kh/JwWjlv2ZcvGfphb" crossorigin="anonymous"></script>
          <style>
            .trdw-notebook-list[x-cloak] { display: none !important; }
            .trdw-notebook-list ol { font-size: 1rem; line-height: 1.6rem; }
          </style>
          <div x-data="$data" class="trdw-notebook-list" :style="{ cursor: busy && 'wait' }" x-cloak>
            <template x-if="disabled">
              <p><code>NotebookList</code> is disabled in a live notebook</p>
            </template>
            <template x-if="busy">
              <p>Fetching the notebook list&hellip;</p>
            </template>
            <template x-if="latestError">
              <p>Failed to fetch the notebook list: <span x-text="latestError.message"></span></p>
            </template>
            <template x-if="!disabled && !busy && !latestError && !hasNotebooks()">
              <p>No notebooks are available</p>
            </template>
            <template x-if="hasNotebooks()">
              <ol>
                <template x-for="notebook in notebooks">
                  <li>
                    <a :href="`./\${notebook.html_path}`" x-text="notebook.frontmatter.title"></a>
                  </li>
                </template>
              </ol>
            </template>
          </div>
        </div>
        """
    )
end
