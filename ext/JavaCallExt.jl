module JavaCallExt

using TRDW
using JavaCall
using Markdown
using Pkg.Artifacts
using Tables
using Dates

# ------------------------------------------------------------
# Java imports
# ------------------------------------------------------------
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
const PackageAccess = @jimport org.apache.poi.openxml4j.opc.PackageAccess
const POIFSFileSystem = @jimport org.apache.poi.poifs.filesystem.POIFSFileSystem
const SXSSFCell = @jimport org.apache.poi.xssf.streaming.SXSSFCell
const SXSSFRow = @jimport org.apache.poi.xssf.streaming.SXSSFRow
const SXSSFSheet = @jimport org.apache.poi.xssf.streaming.SXSSFSheet
const SXSSFWorkbook = @jimport org.apache.poi.xssf.streaming.SXSSFWorkbook

# ------------------------------------------------------------
# Helper macro to manage Java resources with a guaranteed close()
# ------------------------------------------------------------
macro with_java(resource_expr, close_call, body)
    quote
        local __res = $(esc(resource_expr))
        try
            $(esc(body))
        finally
            $(close_call)(__res)
        end
    end
end

# Simple close helper (uses JavaCall.jcall internally)
_close_java_resource(r) = jcall(r, "close", Nothing, ())

# ------------------------------------------------------------
# XLSX writer
# ------------------------------------------------------------
function TRDW.XLSX.write(file, table; password = nothing)
    TRDW.XLSX.write(file, ["Sheet1" => table]; password)
end

function TRDW.XLSX.write(file, sheets::AbstractVector{<:Pair{<:AbstractString}}; password = nothing)
    jcall(IOUtils, "setByteArrayMaxOverride", Nothing, (jint,), typemax(Int32))
    TRDW.XLSX.validate_sheet_names([first(p) for p in sheets])

    workbook = SXSSFWorkbook(())
    try
        # ----------------------------------------------------------------
        # Create reusable helpers (formats, styles, etc.)
        # ----------------------------------------------------------------
        creation_helper = jcall(workbook, "getCreationHelper", CreationHelper, ())

        date_format = jcall(creation_helper, "createDataFormat", DataFormat, ())
        date_fmt_idx = jshort(jcall(date_format, "getFormat", jshort, (JString,), "yyyy-MM-dd"))

        datetime_format = jcall(creation_helper, "createDataFormat", DataFormat, ())
        datetime_fmt_idx = jshort(jcall(datetime_format, "getFormat", jshort, (JString,), "yyyy-MM-dd HH:mm:ss"))

        date_cell_style = jcall(workbook, "createCellStyle", CellStyle, ())
        jcall(date_cell_style, "setDataFormat", Nothing, (jshort,), date_fmt_idx)

        datetime_cell_style = jcall(workbook, "createCellStyle", CellStyle, ())
        jcall(datetime_cell_style, "setDataFormat", Nothing, (jshort,), datetime_fmt_idx)

        wrap_cell_style = jcall(workbook, "createCellStyle", CellStyle, ())
        jcall(wrap_cell_style, "setWrapText", Nothing, (jboolean,), true)

        control_char_locations = Tuple{String, Symbol, Int, Vector{Char}}[]

        # ----------------------------------------------------------------
        # Process each sheet
        # ----------------------------------------------------------------
        for (sheet_name, table) in sheets
            sheet = jcall(workbook, "createSheet", SXSSFSheet, (JString,), sheet_name)
            jcall(sheet, "trackAllColumnsForAutoSizing", Nothing, ())

            sch   = Tables.schema(table)
            cols  = Tables.columnnames(table)
            types = sch.types

            # Set column defaults based on Julia type
            for (i, (c, t)) in enumerate(zip(cols, types))
                nt = Base.nonmissingtype(t)
                if nt <: Dates.Date
                    jcall(sheet, "setDefaultColumnStyle", Nothing,
                          (jint, CellStyle), jint(i-1), date_cell_style)
                elseif nt <: Dates.DateTime
                    jcall(sheet, "setDefaultColumnStyle", Nothing,
                          (jint, CellStyle), jint(i-1), datetime_cell_style)
                end
            end

            # Header row
            header_row = jcall(sheet, "createRow", SXSSFRow, (jint,), jint(0))
            for (i, c) in enumerate(cols)
                cell = jcall(header_row, "createCell", SXSSFCell, (jint,), jint(i-1))
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
                    cell = jcall(row, "createCell", SXSSFCell, (jint,), jint(i-1))

                    if val === missing
                        continue
                    elseif val isa Bool
                        jcall(cell, "setCellValue", Nothing, (jboolean,), val)
                    elseif val isa Dates.Date
                        # Use java.time.LocalDate for pure dates
                        java_date = jcall(LocalDate, "of", LocalDate,
                                          (jint, jint, jint), jint(year(val)), jint(month(val)), jint(day(val)))
                        jcall(cell, "setCellValue", Nothing, (LocalDate,), java_date)
                    elseif val isa Dates.DateTime
                        java_dt = jcall(LocalDateTime, "of", LocalDateTime,
                                        (jint, jint, jint, jint, jint, jint, jint),
                                        jint(year(val)), jint(month(val)), jint(day(val)),
                                        jint(hour(val)), jint(minute(val)), jint(second(val)),
                                        jint(millisecond(val)) * 1_000_000)  # nanoseconds
                        jcall(cell, "setCellValue", Nothing, (LocalDateTime,), java_dt)
                    elseif val isa Number
                        jcall(cell, "setCellValue", Nothing, (jdouble,), Float64(val))
                    else
                        raw = string(val)
                        ctx = "column \"$(string(c))\", row $k"
                        TRDW.XLSX.check_javacall_compatible(raw; context = ctx)
                        TRDW.XLSX.check_cell_length(raw; context = ctx)
                        str = TRDW.XLSX.sanitize_for_xlsx(raw)
                        if str !== raw
                            chars = TRDW.XLSX.find_invalid_control_chars(raw)
                            push!(control_char_locations, (sheet_name, c, k, chars))
                        end
                        if contains(str, '\n')
                            jcall(cell, "setCellStyle", Nothing, (CellStyle,), wrap_cell_style)
                        end
                        jcall(cell, "setCellValue", Nothing, (JString,), str)
                    end
                end
            end

            # Auto‑size columns
            for i in 1:length(cols)
                jcall(sheet, "autoSizeColumn", Nothing, (jint,), jint(i-1))
                width = jcall(sheet, "getColumnWidth", jint, (jint,), jint(i-1))
                width = round(Int, width * TRDW.XLSX.AUTOSIZE_CORRECTION_FACTOR)
                if width > TRDW.XLSX.MAX_COLUMN_WIDTH
                    width = TRDW.XLSX.DEFAULT_COLUMN_WIDTH
                end
                jcall(sheet, "setColumnWidth", Nothing, (jint, jint), jint(i-1), jint(width))
            end
        end

        # Warn about control‑character sanitisation
        if !isempty(control_char_locations)
            n = length(control_char_locations)
            examples = control_char_locations[1:min(3, n)]
            detail = join(["sheet \"$(s)\", column \"$(col)\", row $(r) ($(TRDW.XLSX.describe_codepoints(chars)))"
                           for (s, col, r, chars) in examples], "; ")
            suffix = n > 3 ? " (and $(n - 3) more)" : ""
            @warn "Control characters were replaced with spaces: $detail$suffix"
        end

        # ------------------------------------------------------------
        # Write workbook – encrypted or plain
        # ------------------------------------------------------------
        if password !== nothing
            @with_java ByteArrayOutputStream() _close_java_resource begin
                buffer = __res
                jcall(workbook, "write", Nothing, (OutputStream,), buffer)
                bytes = jcall(buffer, "toByteArray", Vector{jbyte}, ())

                @with_java POIFSFileSystem() _close_java_resource begin
                    fs = __res
                    agile_mode = jfield(EncryptionMode, "agile", EncryptionMode)
                    enc_info = EncryptionInfo((EncryptionMode,), agile_mode)
                    encryptor = jcall(enc_info, "getEncryptor", Encryptor, ())

                    jcall(encryptor, "confirmPassword", Nothing, (JString,), password)

                    @with_java ByteArrayInputStream((Vector{jbyte},), bytes) _close_java_resource begin
                        bais = __res
                        @with_java OPCPackage.open((InputStream,), bais) _close_java_resource begin
                            pkg = __res
                            @with_java encryptor.getDataStream((POIFSFileSystem,), fs) _close_java_resource begin
                                enc_stream = __res
                                jcall(pkg, "save", Nothing, (OutputStream,), enc_stream)
                            end
                        end
                    end

                    @with_java FileOutputStream((JString,), file) _close_java_resource begin
                        fos = __res
                        jcall(fs, "writeFilesystem", Nothing, (OutputStream,), fos)
                    end
                end
            end
        else
            @with_java FileOutputStream((JString,), file) _close_java_resource begin
                fos = __res
                jcall(workbook, "write", Nothing, (OutputStream,), fos)
            end
        end

    finally
        # Ensure temporary files created by SXSSFWorkbook are cleaned up
        success = jcall(workbook, "dispose", jboolean, ())
        success || @warn "SXSSFWorkbook.dispose() failed; temporary files may remain in $(tempdir())"
    end

    return nothing
end

# ------------------------------------------------------------
# OHDSI helper imports
# ------------------------------------------------------------
const CohortExpressionQueryBuilder = @jimport org.ohdsi.circe.cohortdefinition.CohortExpressionQueryBuilder
const BuildExpressionQueryOptions = @jimport org.ohdsi.circe.cohortdefinition.CohortExpressionQueryBuilder$BuildExpressionQueryOptions
const MarkdownRender = @jimport org.ohdsi.circe.cohortdefinition.printfriendly.MarkdownRender
const SqlRender = @jimport org.ohdsi.sql.SqlRender
const SqlTranslate = @jimport org.ohdsi.sql.SqlTranslate
const SqlSplit = @jimport org.ohdsi.sql.SqlSplit

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
    jcall(builder, "buildExpressionQuery", JString, (JString, BuildExpressionQueryOptions), str, nothing)
end

function TRDW.OHDSI.render_sql(template, params = (;))
    jcall(SqlRender, "renderSql", JString, (JString, Vector{JString}, Vector{JString}),
          template,
          collect(String, string.(keys(params))),
          collect(String, string.(values(params))))
end

function TRDW.OHDSI.translate_sql(sql; dialect = "spark", session_id = nothing, temp_emulation_schema = nothing)
    jcall(SqlTranslate, "translateSql", JString, (JString, JString, JString, JString),
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
