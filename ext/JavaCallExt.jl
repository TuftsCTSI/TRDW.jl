module JavaCallExt

using TRDW
using JavaCall
using Markdown
using Pkg.Artifacts
using Tables
using Dates

const ByteArrayInputStream = @jimport java.io.ByteArrayInputStream
const ByteArrayOutputStream = @jimport java.io.ByteArrayOutputStream
const CellStyle = @jimport org.apache.poi.ss.usermodel.CellStyle
const CreationHelper = @jimport org.apache.poi.ss.usermodel.CreationHelper
const DataFormat = @jimport org.apache.poi.ss.usermodel.DataFormat
const EncryptionInfo = @jimport org.apache.poi.poifs.crypt.EncryptionInfo
const EncryptionMode = @jimport org.apache.poi.poifs.crypt.EncryptionMode
const Encryptor = @jimport org.apache.poi.poifs.crypt.Encryptor
const File = @jimport java.io.File
const FileOutputStream = @jimport java.io.FileOutputStream
const InputStream = @jimport java.io.InputStream
const IOUtils = @jimport org.apache.poi.util.IOUtils
const LocalDate = @jimport java.time.LocalDate
const LocalDateTime = @jimport java.time.LocalDateTime
const OPCPackage = @jimport org.apache.poi.openxml4j.opc.OPCPackage
const OutputStream = @jimport java.io.OutputStream
const POIFSFileSystem = @jimport org.apache.poi.poifs.filesystem.POIFSFileSystem
const SXSSFCell = @jimport org.apache.poi.xssf.streaming.SXSSFCell
const SXSSFRow = @jimport org.apache.poi.xssf.streaming.SXSSFRow
const SXSSFSheet = @jimport org.apache.poi.xssf.streaming.SXSSFSheet
const SXSSFWorkbook = @jimport org.apache.poi.xssf.streaming.SXSSFWorkbook

const MarkdownRender = @jimport org.ohdsi.circe.cohortdefinition.printfriendly.MarkdownRender
const SqlRender = @jimport org.ohdsi.sql.SqlRender
const SqlTranslate = @jimport org.ohdsi.sql.SqlTranslate
const SqlSplit = @jimport org.ohdsi.sql.SqlSplit

function with_java(resource_expr::Function, close_method::String, body::Function)
    __java_res = resource_expr()
    try
        body(__java_res)
    finally
        jcall(__java_res, close_method, Nothing, ())
    end
end

function _create_style(workbook::JavaObject, fmt_idx::jshort)
    style = jcall(workbook, "createCellStyle", CellStyle, ())
    jcall(style, "setDataFormat", Nothing, (jshort,), fmt_idx)
    style
end

function _setup_styles(workbook::JavaObject)
    ch = jcall(workbook, "getCreationHelper", CreationHelper, ())

    date_fmt = jcall(ch, "createDataFormat", DataFormat, ())
    date_idx = jshort(jcall(date_fmt, "getFormat", jshort, (JString,), "yyyy-MM-dd"))

    datetime_fmt = jcall(ch, "createDataFormat", DataFormat, ())
    datetime_idx = jshort(jcall(datetime_fmt, "getFormat", jshort, (JString,), "yyyy-MM-dd HH:mm:ss"))

    date_style = _create_style(workbook, date_idx)
    datetime_style = _create_style(workbook, datetime_idx)

    wrap_style = jcall(workbook, "createCellStyle", CellStyle, ())
    jcall(wrap_style, "setWrapText", Nothing, (jboolean,), true)

    (date_style = date_style,
        datetime_style = datetime_style,
        wrap_style = wrap_style)
end

function _write_cell!(
        cell::JavaObject,
        val,
        col_sym::Symbol,
        row_idx::Int,
        sheet_name::String,
        control_char_locations::Vector{Tuple{String, Symbol, Int, Vector{Char}}},
        workbook::JavaObject,
        styles::NamedTuple)
    try
        if val === missing
            return
        elseif val isa Bool
            jcall(cell, "setCellValue", Nothing, (jboolean,), val)

        elseif val isa Dates.Date
            java_date = jcall(
                LocalDate, "of", LocalDate,
                (jint, jint, jint),
                jint(year(val)), jint(month(val)), jint(day(val))
            )
            jcall(cell, "setCellValue", Nothing, (LocalDate,), java_date)

        elseif val isa Dates.DateTime
            nano = jlong(millisecond(val)) * 1_000_000
            java_dt = jcall(
                LocalDateTime, "of", LocalDateTime,
                (jint, jint, jint, jint, jint, jint, jint),
                jint(year(val)), jint(month(val)), jint(day(val)),
                jint(hour(val)), jint(minute(val)), jint(second(val)), nano
            )
            jcall(cell, "setCellValue", Nothing, (LocalDateTime,), java_dt)

        elseif val isa Integer
            jcall(cell, "setCellValue", Nothing, (jdouble,), jdouble(val))

        elseif val isa AbstractFloat
            jcall(cell, "setCellValue", Nothing, (jdouble,), Float64(val))

        else
            raw = string(val)
            ctx = "column \"$(string(col_sym))\", row $row_idx"
            TRDW.XLSX.check_javacall_compatible(raw; context = ctx)
            TRDW.XLSX.check_cell_length(raw; context = ctx)
            str = TRDW.XLSX.sanitize_for_xlsx(raw)

            if str !== raw
                chars = TRDW.XLSX.find_invalid_control_chars(raw)
                push!(control_char_locations,
                    (sheet_name, col_sym, row_idx, chars))
            end

            if contains(str, '\n')
                jcall(cell, "setCellStyle", Nothing, (CellStyle,), styles.wrap_style)
            end
            jcall(cell, "setCellValue", Nothing, (JString,), str)
        end
    catch e
        @error "Failed to write cell (sheet=$(sheet_name), column=$(col_sym), row=$(row_idx))" exception=(e,)
    end
end

function TRDW.XLSX.write(file, table; password = nothing)
    TRDW.XLSX.write(file, ["Sheet1" => table]; password)
end

function TRDW.XLSX.write(file, sheets::AbstractVector{<:Pair{<:AbstractString}}; password = nothing)
    jcall(IOUtils, "setByteArrayMaxOverride", Nothing, (jint,), typemax(Int32))
    TRDW.XLSX.validate_sheet_names([first(p) for p in sheets])

    workbook = SXSSFWorkbook(())

    # Global helpers (formats, styles, etc.)
    styles = _setup_styles(workbook)
    control_char_locations = Tuple{String, Symbol, Int, Vector{Char}}[]

    # Process each sheet
    for (sheet_name, table) in sheets
        sheet = jcall(workbook, "createSheet", SXSSFSheet, (JString,), sheet_name)
        jcall(sheet, "trackAllColumnsForAutoSizing", Nothing, ())

        sch   = Tables.schema(table)
        cols  = Tables.columnnames(table)
        types = sch.types

        # Set column defaults based on Julia type
        for (i, (_, t)) in enumerate(zip(cols, types))
            nt = Base.nonmissingtype(t)
            if nt <: Dates.Date
                jcall(sheet, "setDefaultColumnStyle", Nothing,
                    (jint, CellStyle), jint(i - 1), styles.date_style)
            elseif nt <: Dates.DateTime
                jcall(sheet, "setDefaultColumnStyle", Nothing,
                    (jint, CellStyle), jint(i - 1), styles.datetime_style)
            end
        end

        # Header row
        header_row = jcall(sheet, "createRow", SXSSFRow, (jint,), jint(0))
        for (i, c) in enumerate(cols)
            cell = jcall(header_row, "createCell", SXSSFCell, (jint,), jint(i - 1))
            header = TRDW.XLSX.decode_funsql_label(string(c))
            TRDW.XLSX.check_javacall_compatible(header; context = "column header \"$header\"")
            TRDW.XLSX.check_cell_length(header; context = "column header \"$header\"")
            header = TRDW.XLSX.sanitize_for_xlsx(header)
            jcall(cell, "setCellValue", Nothing, (JString,), header)
        end

        # Data rows
        for (k, r) in enumerate(Tables.rows(table))
            row = jcall(sheet, "createRow", SXSSFRow, (jint,), jint(k))
            for (i, c) in enumerate(Tables.columnnames(r))
                val = Tables.getcolumn(r, c)
                cell = jcall(row, "createCell", SXSSFCell, (jint,), jint(i - 1))
                _write_cell!(cell, val, c, k, sheet_name,
                    control_char_locations, workbook, styles)
            end
        end

        # Auto‑size columns
        for i in 1:length(cols)
            jcall(sheet, "autoSizeColumn", Nothing, (jint,), jint(i - 1))
            width = jcall(sheet, "getColumnWidth", jint, (jint,), jint(i - 1))
            width = round(Int, width * TRDW.XLSX.AUTOSIZE_CORRECTION_FACTOR)
            if width > TRDW.XLSX.MAX_COLUMN_WIDTH
                width = TRDW.XLSX.DEFAULT_COLUMN_WIDTH
            end
            jcall(sheet, "setColumnWidth", Nothing, (jint, jint),
                jint(i - 1), jint(width))
        end
    end

    # Warn about control‑character sanitisation
    if !isempty(control_char_locations)
        n = length(control_char_locations)
        examples = control_char_locations[1:min(3, n)]
        detail = join(
            ["sheet \"$(s)\", column \"$(col)\", row $(r) ($(TRDW.XLSX.describe_codepoints(chars)))"
                for (s, col, r, chars) in examples],
            "; "
        )
        suffix = n > 3 ? " (and $(n - 3) more)" : ""
        @warn "Control characters were replaced with spaces: $detail$suffix"
    end

    # Write final workbook
    if password !== nothing
        with_java(() -> ByteArrayOutputStream(), "close") do __java_res
            buffer = __java_res
            jcall(workbook, "write", Nothing, (OutputStream,), buffer)
            bytes = jcall(buffer, "toByteArray", Vector{jbyte}, ())

            with_java(() -> POIFSFileSystem(), "close") do __java_res
                fs = __java_res
                agile_mode = jfield(EncryptionMode, "agile", EncryptionMode)
                enc_info = EncryptionInfo((EncryptionMode,), agile_mode)
                encryptor = jcall(enc_info, "getEncryptor", Encryptor, ())

                jcall(encryptor, "confirmPassword", Nothing, (JString,), password)

                with_java(() -> ByteArrayInputStream((Vector{jbyte},), bytes), "close") do __java_res
                    bais = __java_res
                    with_java(() -> OPCPackage.open((InputStream,), bais), "close") do __java_res
                        pkg = __java_res
                        with_java(() -> encryptor.getDataStream((POIFSFileSystem,), fs), "close") do __java_res
                            enc_stream = __java_res
                            jcall(pkg, "save", Nothing, (OutputStream,), enc_stream)
                        end
                    end
                end

                with_java(() -> FileOutputStream((JString,), file), "close") do __java_res
                    fos = __java_res
                    jcall(fs, "writeFilesystem", Nothing, (OutputStream,), fos)
                end
            end
        end
    else
        with_java(() -> FileOutputStream((JString,), file), "close") do __java_res
            fos = __java_res
            jcall(workbook, "write", Nothing, (OutputStream,), fos)
        end
    end

    success = Bool(jcall(workbook, "dispose", jboolean, ()))
    success || @warn "SXSSFWorkbook.dispose() failed; temporary files may remain in $(tempdir())"

    return nothing
end

function TRDW.OHDSI.cohort_definition_to_md(str)
    mr = MarkdownRender(())
    jcall(mr, "renderCohort", JString, (JString,), str) |> Markdown.parse
end

function TRDW.OHDSI.concept_set_list_definition_to_md(str)
    mr = MarkdownRender(())
    jcall(mr, "renderConceptSetList", JString, (JString,), str) |> Markdown.parse
end

function TRDW.OHDSI.concept_set_definition_to_md(str)
    mr = MarkdownRender(())
    jcall(mr, "renderConceptSet", JString, (JString,), str) |> Markdown.parse
end

function TRDW.OHDSI.cohort_definition_to_sql_template(str)
    builder = CohortExpressionQueryBuilder(())
    jcall(builder, "buildExpressionQuery", JString,
        (JString, BuildExpressionQueryOptions), str, nothing)
end

function TRDW.OHDSI.render_sql(template, params = (;))
    jcall(SqlRender, "renderSql", JString,
        (JString, Vector{JString}, Vector{JString}),
        template,
        collect(String, string.(keys(params))),
        collect(String, string.(values(params))))
end

function TRDW.OHDSI.translate_sql(sql; dialect = "spark", session_id = nothing,
        temp_emulation_schema = nothing)
    jcall(SqlTranslate, "translateSql", JString,
        (JString, JString, JString, JString),
        sql, dialect,
        session_id !== nothing ? string(session_id) : nothing,
        temp_emulation_schema !== nothing ? string(temp_emulation_schema) : nothing)
end

function TRDW.OHDSI.split_sql(sql)
    v = jcall(SqlSplit, "splitSql", Vector{JString}, (JString,), sql)
    map(JavaCall.unsafe_string, v)
end

function __init__()
    JavaCall.addClassPath(joinpath(artifact"csv2xlsx", "*"))
    JavaCall.addClassPath(joinpath(artifact"CirceR", "CirceR-1.3.2/inst/java/*"))
    JavaCall.addClassPath(joinpath(artifact"SqlRender", "SqlRender-1.16.1/inst/java/*"))
end

end # module JavaCallExt
