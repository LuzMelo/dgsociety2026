# ============================================
# SCRIPT 1: FULL PDF TEXT EXTRACTION
# Extracts ALL text from PDFs
# ============================================

library(pdftools)
library(tidyverse)

# Set your PDF folder path
pdf_folder <- "C:/Users/luzya/OneDrive/Escritorio/DGO_2026/data/90-FR-17835-combined-responses"

# Get list of all PDF files
pdf_files <- list.files(pdf_folder, pattern = "\\.pdf$", full.names = TRUE)

print(paste("Found", length(pdf_files), "PDF files"))

# Function to extract ALL text from PDF
extract_full_pdf_text <- function(pdf_path) {
  tryCatch({
    text <- pdf_text(pdf_path)
    combined_text <- paste(text, collapse = "\n\n")
    return(combined_text)
  }, error = function(e) {
    warning(paste("Failed to extract:", basename(pdf_path)))
    return(NA_character_)
  })
}

# Function to extract organization from filename
parse_org_from_filename <- function(filename) {
  base_name <- basename(filename)
  base_name <- gsub("\\.pdf$", "", base_name)
  
  # Format: AI-RD-SP-RFI-2025-XXXX-OrgName
  parts <- str_split(base_name, "-")[[1]]
  if (length(parts) >= 6) {
    org_name <- paste(parts[6:length(parts)], collapse = "-")
  } else {
    org_name <- base_name
  }
  
  return(org_name)
}

# Function to extract submitter name from text
extract_submitter_name <- function(full_text) {
  if (is.na(full_text)) return(NA_character_)
  
  # Look for "Name:" field
  pattern <- "Name:\\s*([^\n]+)"
  match <- str_extract(full_text, pattern)
  
  if (!is.na(match)) {
    name <- str_replace(match, "Name:\\s*", "")
    return(str_trim(name))
  }
  
  return(NA_character_)
}

# Initialize dataframe
print("Creating initial dataframe...")
submissions_data <- tibble(
  file_path = pdf_files,
  file_name = basename(pdf_files)
)

# ADD organization from filename
print("Parsing organization names from filenames...")
submissions_data$org_from_filename <- sapply(submissions_data$file_path, parse_org_from_filename)
# REMOVE sapply-generated names (file paths)
submissions_data$org_from_filename <- unname(submissions_data$org_from_filename)
# CLEAN org names: remove leading 4 digits + dash
submissions_data$org_from_filename <- gsub("^\\d{4}-", "", submissions_data$org_from_filename)

# Extract FULL text from all PDFs
print("Extracting FULL text from all PDFs (including attachments)...")
print("This will take 5-10 minutes...")

submissions_data$full_text <- sapply(seq_along(submissions_data$file_path), function(i) {
  if (i %% 50 == 0) {
    cat(sprintf("\nProcessing: %d/%d (%.1f%%)", i, length(submissions_data$file_path), 
                (i/length(submissions_data$file_path))*100))
  } else {
    cat(".")
  }
  extract_full_pdf_text(submissions_data$file_path[i])
})

# Extract submitter names
print("\nExtracting submitter names...")
submissions_data$submitter_name <- sapply(submissions_data$full_text, extract_submitter_name)

# Add metadata
submissions_data$submission_id <- seq_len(nrow(submissions_data))
submissions_data$text_length <- nchar(submissions_data$full_text)
submissions_data$has_text <- !is.na(submissions_data$full_text) & submissions_data$text_length > 100

# Summary statistics
cat("\n\n")
cat(paste(rep("=", 50), collapse = ""), "\n")
cat("EXTRACTION SUMMARY:\n")
cat(paste("Total PDFs:", nrow(submissions_data), "\n"))
cat(paste("Successfully extracted:", sum(submissions_data$has_text), "\n"))
cat(paste("Failed extractions:", sum(!submissions_data$has_text), "\n"))
cat(paste("Average text length:", round(mean(submissions_data$text_length, na.rm = TRUE)), "\n"))
cat(paste("Median text length:", round(median(submissions_data$text_length, na.rm = TRUE)), "\n"))

# Text length distribution
cat("\nText length distribution:\n")
cat(paste("Min:", min(submissions_data$text_length, na.rm = TRUE), "\n"))
cat(paste("Q1:", quantile(submissions_data$text_length, 0.25, na.rm = TRUE), "\n"))
cat(paste("Median:", median(submissions_data$text_length, na.rm = TRUE), "\n"))
cat(paste("Q3:", quantile(submissions_data$text_length, 0.75, na.rm = TRUE), "\n"))
cat(paste("Max:", max(submissions_data$text_length, na.rm = TRUE), "\n"))

# Create clean output
output_data <- submissions_data %>%
  select(
    submission_id,
    file_name,
    org_from_filename,
    submitter_name,
    full_text,
    text_length
  )

# Save to both CSV and RDS in your folder
output_csv <- "C:/Users/luzya/OneDrive/Escritorio/DGO_2026/data/submissions_full_text.csv"
output_rds <- "C:/Users/luzya/OneDrive/Escritorio/DGO_2026/data/submissions_full_text.rds"

write_csv(output_data, output_csv)
saveRDS(output_data, output_rds)

cat(paste("\n✓ Saved CSV to:", output_csv, "\n"))
cat(paste("✓ Saved RDS to:", output_rds, "\n"))
cat("✓ Ready for Claude API processing!\n")

# Preview
cat("\nFirst 3 submissions:\n")
print(head(output_data %>% select(submission_id, org_from_filename, submitter_name, text_length), 3))

# Sample text
cat("\nSample submission (first 500 chars):\n")
sample_text <- output_data$full_text[1]
if (!is.na(sample_text)) {
  cat(substr(sample_text, 1, min(500, nchar(sample_text))), "\n")
}
