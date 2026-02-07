# =============================================================================
# RFI Recommendation Extractor - OPTIMIZED VERSION
# Using Claude Batch API + Parallel Processing + Crash Recovery
# =============================================================================

library(httr2)
library(jsonlite)
library(tidyverse)
library(furrr)
library(future)

# =============================================================================
# CONFIGURATION
# =============================================================================

ANTHROPIC_API_KEY <- "YOUR_API_KEY_HERE"
MODEL <- "claude-sonnet-4-20250514"

# Batch API endpoints
BATCH_API_URL <- "https://api.anthropic.com/v1/messages/batches"

# Crash recovery configuration
BATCH_SAVE_INTERVAL <- 10  # Save progress every N documents
PROGRESS_DIR <- "temp_progress"
PROGRESS_FILE <- file.path(PROGRESS_DIR, "processing_progress.txt")

# Memory management
options(future.globals.maxSize = 2000 * 1024^2)  # 2GB limit for futures

# =============================================================================
# CRASH RECOVERY HELPER FUNCTIONS
# =============================================================================

# Initialize progress directory
init_progress_tracking <- function() {
  dir.create(PROGRESS_DIR, showWarnings = FALSE, recursive = TRUE)
  message(sprintf("📁 Progress directory: %s", PROGRESS_DIR))
}

# Write progress message to console and log file
write_progress <- function(msg) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- paste0("[", timestamp, "] ", msg)
  message(full_msg)
  
  # Append to log file
  cat(full_msg, "\n", file = PROGRESS_FILE, append = TRUE)
}

# Save data safely with backup
safe_save <- function(data, filepath) {
  # Save to temp file first
  
  temp_file <- paste0(filepath, ".tmp")
  saveRDS(data, temp_file)
  
  # Then rename (atomic operation)
  file.rename(temp_file, filepath)
  
  write_progress(sprintf("💾 Saved: %s", basename(filepath)))
}

# Get list of completed document IDs from saved batches
get_completed_doc_ids <- function() {
  batch_files <- list.files(PROGRESS_DIR, pattern = "^batch_.*\\.rds$", full.names = TRUE)
  
  if (length(batch_files) == 0) {
    return(character(0))
  }
  
  completed_ids <- character(0)
  
  for (f in batch_files) {
    tryCatch({
      batch_data <- readRDS(f)
      if (!is.null(batch_data) && "doc_id" %in% names(batch_data)) {
        completed_ids <- c(completed_ids, unique(batch_data$doc_id))
      }
    }, error = function(e) {
      write_progress(sprintf("⚠️ Could not read %s: %s", basename(f), e$message))
    })
  }
  
  return(unique(completed_ids))
}

# Load all saved batch results
load_all_saved_batches <- function() {
  batch_files <- list.files(PROGRESS_DIR, pattern = "^batch_.*\\.rds$", full.names = TRUE)
  
  if (length(batch_files) == 0) {
    return(NULL)
  }
  
  write_progress(sprintf("📂 Loading %d saved batch files...", length(batch_files)))
  
  all_data <- list()
  
  for (f in batch_files) {
    tryCatch({
      batch_data <- readRDS(f)
      if (!is.null(batch_data) && nrow(batch_data) > 0) {
        all_data[[basename(f)]] <- batch_data
      }
    }, error = function(e) {
      write_progress(sprintf("⚠️ Could not read %s: %s", basename(f), e$message))
    })
  }
  
  if (length(all_data) > 0) {
    combined <- bind_rows(all_data)
    write_progress(sprintf("✅ Loaded %d recommendations from %d documents", 
                           nrow(combined), n_distinct(combined$doc_id)))
    return(combined)
  }
  
  return(NULL)
}

# Save progress state (list of completed doc_ids)
save_progress_state <- function(completed_doc_ids) {
  state_file <- file.path(PROGRESS_DIR, "completed_docs.rds")
  safe_save(completed_doc_ids, state_file)
}

# Load progress state
load_progress_state <- function() {
  state_file <- file.path(PROGRESS_DIR, "completed_docs.rds")
  if (file.exists(state_file)) {
    return(readRDS(state_file))
  }
  return(character(0))
}

# =============================================================================
# PROMPT TEMPLATE (same as before)
# =============================================================================

create_extraction_prompt <- function(doc_id, org_name, document_text) {
  paste0(
    'You are an expert policy analyst specializing in AI governance, human-centered AI, and technology policy. You have extensive experience analyzing public comments and RFI submissions to extract actionable policy recommendations.

BACKGROUND:
You are analyzing submissions to the 2025 National AI R&D Strategic Plan Request for Information (RFI). These are policy documents from various organizations providing input on U.S. AI research and development priorities. Your task is to systematically extract policy recommendations and identify human-centered AI (HCAI) principles.

TASK METHODOLOGY:
1. Read the entire document carefully to understand context
2. Identify each distinct policy recommendation
3. For each recommendation, extract structured information
4. Classify the submitting organization
5. Assign relevant policy topics with justifications
6. Identify HCAI elements (values, properties, purposes) present in each recommendation with justifications

DOCUMENT METADATA:
- Document ID: ', doc_id, '
- Organization Name: ', org_name, '

INPUT DATA FORMAT:
The text below is extracted from a PDF submission. Each submission may contain multiple recommendations presented in various formats (prose paragraphs, bullet points, numbered lists, or section headers).

---
DOCUMENT TEXT:
', document_text, '
---

ANALYTICAL PROCESS:

STEP 1 - ORGANIZATION CLASSIFICATION:
Classify the organization into exactly ONE of these types based on the organization name and document content:

- "Anonymous": Submissions explicitly labeled as anonymous with no identifying organizational information.
- "Individual / Personal Submission": Submissions attributed to named individuals without an organizational affiliation.
- "Academia": Universities, colleges, and academic research institutions focused on education and scholarly research.
- "AI Company": For-profit firms whose primary products or services involve AI-based technologies or systems.
- "Frontier Developer": Organizations developing large-scale, state-of-the-art or foundational AI models.
- "Industry (Other)": For-profit organizations where AI is not the primary product or organizational focus.
- "Industry Association": Membership-based organizations representing the collective interests of firms within an industry or sector.
- "Professional Society": Individual membership organizations supporting professional standards, conferences, and scholarly exchange.
- "Think Tank / Policy Research": Research organizations producing policy analysis, reports, and strategic recommendations.
- "Advocacy Organization": Mission-driven organizations focused on normative goals, public engagement, or policy advocacy.
- "Nonprofit (Other)": Nonprofit organizations not primarily categorized as advocacy, professional, or policy research entities.

STEP 2 - RECOMMENDATION EXTRACTION:
For EACH distinct policy recommendation found, extract:

1. "recommendation": A short title (5-10 words, e.g., "Build Highly Secure AI Infrastructure for Defense")

2. "summary": Provide a 3-6 sentence explanation of what this proposes and how it would be implemented. If the original submission presents this recommendation with multiple specific components, actions, or a list, format the summary as a brief intro followed by bullet points. Example format:
   "Enact a comprehensive American Data Management & Artificial Intelligence Act (ADMAIA) that would: • Standardize data rights and consent rules • Implement privacy by design principles including a CLEAR Label system • Preempt all state data management and AI laws • Establish responsible AI development principles • Provide a sound liability regime without private cause of action"

3. "justification": 2-5 sentences explaining why this recommendation is important, according to the submission. Include specific reasoning and, if possible, an EXACT quotation from the submission. Example format:
   "The submitter argues this is critical because current AI systems lack sufficient oversight mechanisms for high-stakes decisions. They emphasize that \'without robust accountability frameworks, AI-driven errors in healthcare and criminal justice will continue to disproportionately harm vulnerable populations.\' The submission cites recent incidents where algorithmic failures resulted in wrongful denials of benefits, underscoring the urgency of federal intervention."

4. "topics": An array of 1-3 topic objects, ordered by relevance (most relevant first). Each topic object contains:
   - "topic_id": The ranking (1 = most relevant, 2 = second most relevant, 3 = third most relevant)
   - "topic": The exact topic name from the list below
   - "topic_justification": 3-6 sentences explaining why this recommendation matches this topic, with specific references to the recommendation content and how it aligns with the topic definition.

STEP 3 - HCAI ELEMENT EXTRACTION:
For each recommendation, identify which Human-Centered AI elements are explicitly or implicitly addressed. Extract up to 3 elements for each category, ordered by relevance:

5. "hcai_values": An array of 1-3 value objects, ordered by relevance (most relevant first). Each object contains:
   - "value_id": The ranking (1 = most relevant, 2 = second, 3 = third)
   - "value": The exact value name from the list below
   - "value_justification": 3–9 sentences explaining why this recommendation reflects this value and how it aligns with the value definition. Including AT LEAST one direct quote from the document. Example format: 
   "This recommendation reflects the HCAI value of Fairness by explicitly addressing the risk of discriminatory outcomes in AI-driven decision-making systems. The submission emphasizes that AI tools must not “replicate or amplify historical inequities embedded in existing datasets,” highlighting a concern with unequal treatment across demographic groups. The recommendation calls for mandatory bias audits and demographic performance reporting, which are concrete mechanisms aimed at ensuring equitable treatment. By stating that “AI systems used in hiring, lending, or healthcare must demonstrate comparable accuracy across protected classes,” the document directly links fairness to measurable outcomes. The emphasis on pre-deployment testing further shows an intent to prevent discrimination before systems are widely adopted. Together, these elements demonstrate a clear commitment to fairness as an ethical requirement rather than an aspirational goal. The recommendation situates fairness as a core design constraint throughout development and deployment."

   HCAI VALUES:
   ETHICAL:
   - "Dignity": Respect for the inherent worth and value of humans throughout AI development, deployment, and use
   - "Fairness": Equitable treatment of humans and absence of discrimination or bias in AI design, development, and deployment
   - "Justice": Societal considerations of distributing benefits, burdens, and opportunities of AI systems in an impartial and rightful manner
   PROTECTION:
   - "Privacy": Implementing measures to collect, store, and process data respecting regulations and individuals consent
   - "Safety": Identifying and addressing risks and threats such as biased decision-making, privacy breaches, or unintended consequences
   - "Prevention of Harm": Proactive measures to identify and address potential risks and negative impacts by anticipating and mitigating unintended consequences
   PERFORMANCE:
   - "Efficiency": Ability to accomplish tasks with minimal wasted effort, time, or resources through automation, autonomous action, or augmentation
   - "Effectiveness": Ability of AI systems to achieve desired outcomes and deliver value in alignment with identified needs and goals
   - "Accuracy": Degree to which AI systems can produce correct and reliable results

6. "hcai_properties": An array of 1-3 property objects, ordered by relevance (most relevant first). Each object contains:
   - "property_id": The ranking (1 = most relevant, 2 = second, 3 = third)
   - "property": The exact property name from the list below
   - "property_justification": 3–9 sentences explaining why this recommendation reflects this property and how it aligns with the property definition. Including AT LEAST one direct quote from the document.Example format:
   "This recommendation strongly addresses the HCAI property of Transparency by requiring disclosure of AI system design choices, data sources, and limitations. The submission argues that “users and regulators must be able to understand how automated decisions are made and what data informs them.” It proposes standardized documentation that explains model purpose, training data provenance, and known failure modes. By calling for publicly accessible system impact statements, the recommendation reduces informational asymmetries between developers and affected communities. The document further notes that transparency is essential for accountability, stating that “opaque systems undermine meaningful oversight.” These provisions collectively enable external scrutiny and informed evaluation of AI systems. The recommendation therefore operationalizes transparency as a structural feature rather than a voluntary practice."

   HCAI PROPERTIES:
   OVERSIGHT:
   - "Accountability": Answerability, liability, and ultimate power of decisions regarding AI design, development, and deployment
   - "Controllability": Ability of human users to exert influence, manage, and oversee AI behavior and actions
   - "Responsibility": Moral duty and obligation to act in consideration of individual and societal interests, rights, and well-being
   - "Sustainability": Long-term viability considering environmental, social, and economic aspects to align with sustainable development goals
   COMPREHENSION:
   - "Explainability": Ability to enable users to make sense of AI inner workings through clear explanations for decisions and actions
   - "Intelligibility": Ability to provide clarity on AI system allowing humans to interpret reasoning, processes, and outputs
   - "Transparency": Openness and insight on processes, algorithms, data sources, and intentions behind AI systems
   INTEGRITY:
   - "Reliability": Level of consistency and dependability of AI systems in delivering accurate and trustworthy results
   - "Robustness": Ability to maintain performance even when faced with unexpected or adversarial conditions
   - "Trustworthiness": Instilling confidence and reliance among human users when using an AI system
   - "Traceability": Ability to track processing, generated outputs, and data sources of an AI system

7. "hcai_purposes": An array of 1-3 purpose objects, ordered by relevance (most relevant first). Each object contains:
   - "purpose_id": The ranking (1 = most relevant, 2 = second, 3 = third)
   - "purpose": The exact purpose name from the list below
   - "purpose_justification": 3–9 sentences explaining why this recommendation reflects this purpose and how it aligns with the purpose definition. Including AT LEAST one direct quote from the document. Example format:
   "This recommendation aligns most closely with the HCAI purpose of Augmentation, as it frames AI systems as tools to support rather than replace human judgment. The submission explicitly states that AI should be used to “assist human experts by synthesizing large volumes of information, not to supplant professional decision-making.” It emphasizes that final authority should remain with trained practitioners, particularly in high-stakes contexts such as healthcare and public benefits administration. The recommendation proposes decision-support interfaces that surface explanations and alternative options, reinforcing human agency. By arguing that AI can “enhance efficiency while preserving human discretion,” the document positions automation as complementary rather than substitutive. This framing clearly situates AI as a capability-enhancing technology. As such, the recommendation advances augmentation as the primary purpose guiding system design and deployment."

   HCAI PURPOSES:
   - "Augmentation": Use of AI to enhance and amplify human capabilities, skills, and decision-making, empowering humans and improving productivity
   - "AI-Autonomy": Capacity of AI to manifest agency via performing actions, operating independently of humans to achieve goals with flexibility
   - "Automation": Undertaking of well-defined tasks by AI that can perform them with no human intervention

TOPIC DEFINITIONS (use exact names only):
- "Standards and Regulations": Recommendations for government development of standards for AI systems, providing guidelines to industry, or passing harder requirements and regulations for how industry needs to operate (other than export controls).
- "Infrastructure": Recommendations addressing computing resources, data centers, energy requirements, cooling technology, chip manufacturing, and other physical requirements for AI development.
- "Data & IP": Recommendations related to data collection, provenance, privacy, as well as IP protection and licensing.
- "Security": Recommendations addressing security vulnerabilities and defenses across the AI technology stack, including model weight security, insider threat programs, hardware security, and security standard development.
- "Talent & Education": Recommendations on talent development, education, training, reskilling initiatives, STEM programs, immigration reforms, and preparing workers for AI-driven economic changes.
- "Evidence of Risks": Recommendations related to testing, evaluating, and documenting AI risks, including transparency requirements, model evaluations, incident reporting, whistleblower programs, and methods for understanding AI system behavior.
- "Deregulation": Recommendations promoting regulatory streamlining, including federal preemption, liability shields, and sandboxes.
- "Ethics & Civil Rights": Recommendations addressing fairness, bias, discrimination, content moderation, platform responsibility, and ensuring AI respects human rights.
- "Healthcare": Recommendations for healthcare applications of AI, including medical diagnostics, drug discovery, clinical decision support, patient data governance, and healthcare delivery optimization.
- "Procurement": Recommendations related to government acquisition of AI systems, procurement requirements, contracting processes, and public sector adoption of AI technologies.
- "Basic Science": Recommendations related to foundational R&D, scientific research, and mechanisms/reforms to support better research.
- "Market Impacts": Recommendations addressing the economic effects of AI, including competition policy, productivity impacts, wealth distribution, labor market effects, and transition support.
- "Global Engagement": Recommendations for international coordination, competition, and diplomatic efforts around AI (distinct from export controls).
- "Export Controls": Recommendations specifically addressing regulations on international transfers of AI technologies, including tooling, chips, and software.
- "Other Sectors": Recommendations for AI applications in specific domains not covered by other topics (e.g., transportation, agriculture, legal services, manufacturing).
- "Open Source": Recommendations related to open-weight releases, permissive licensing, open model development, and transparency in model architecture.
- "Military": Recommendations focused on defense applications of AI, autonomous weapons systems, battlefield decision support, and military training with AI.
- "Finance": Recommendations addressing AI in financial services, including algorithmic trading, fraud detection, credit scoring, and insurance underwriting.
- "Biosecurity": Recommendations focused on preventing biological risks from AI.

OUTPUT FORMAT:
Return ONLY valid JSON with no markdown formatting or additional explanation. The output must be parseable by standard JSON parsers.

{
  "doc_id": "', doc_id, '",
  "org_name": "', org_name, '",
  "org_type": "",
  "total_recommendations": N,
  "recommendations": [
    {
      "id": 1,
      "recommendation": "",
      "summary": "",
      "justification": "",
      "topics": [
        {
          "topic_id": 1,
          "topic": "Primary Topic",
          "topic_justification": ""
        }
      ],
      "hcai_values": [
        {
          "value_id": 1,
          "value": "Primary Value",
          "value_justification": ""
        }
      ],
      "hcai_properties": [
        {
          "property_id": 1,
          "property": "Primary Property",
          "property_justification": ""
        }
      ],
      "hcai_purposes": [
        {
          "purpose_id": 1,
          "purpose": "Primary Purpose",
          "purpose_justification": ""
        }
      ]
    }
  ]
}')
}

# =============================================================================
# LOAD DATA FROM RDS
# =============================================================================

load_submissions_from_rds <- function(rds_path) {
  df <- readRDS(rds_path)
  
  documents <- df |>
    transmute(
      doc_id = as.character(submission_id),
      org_name = org_from_filename,
      text = full_text
    )
  
  return(documents)
}

# =============================================================================
# METHOD 1: BATCH API (Recommended for large jobs - 50% cheaper)
# =============================================================================

# Create batch request file (JSONL format)
create_batch_requests <- function(documents, output_file = "batch_requests.jsonl") {
  
  # Create requests list
  requests <- pmap(
    list(documents$doc_id, documents$org_name, documents$text),
    function(doc_id, org_name, text) {
      list(
        custom_id = doc_id,
        params = list(
          model = MODEL,
          max_tokens = 16000,
          messages = list(
            list(
              role = "user",
              content = create_extraction_prompt(doc_id, org_name, text)
            )
          )
        )
      )
    }
  )
  
  # Write as JSONL
  jsonl_lines <- sapply(requests, function(r) toJSON(r, auto_unbox = TRUE))
  writeLines(jsonl_lines, output_file)
  
  message(sprintf("Created batch request file: %s (%d requests)", output_file, length(requests)))
  return(output_file)
}

# Submit batch job
submit_batch <- function(jsonl_file) {
  
  # Read the JSONL file
  jsonl_content <- readLines(jsonl_file)
  
  response <- request(BATCH_API_URL) |>
    req_headers(
      `x-api-key` = ANTHROPIC_API_KEY,
      `anthropic-version` = "2023-06-01",
      `anthropic-beta` = "message-batches-2024-09-24",
      `Content-Type` = "application/json"
    ) |>
    req_body_json(list(
      requests = lapply(jsonl_content, function(line) fromJSON(line))
    )) |>
    req_perform()
  
  result <- resp_body_json(response)
  
  message(sprintf("Batch submitted! ID: %s", result$id))
  message(sprintf("Status: %s", result$processing_status))
  
  return(result)
}

# Alternative: Upload file and submit batch
submit_batch_from_file <- function(jsonl_file) {
  
  # Read JSONL and parse each line
  lines <- readLines(jsonl_file)
  requests <- lapply(lines, fromJSON)
  
  response <- request(BATCH_API_URL) |>
    req_headers(
      `x-api-key` = ANTHROPIC_API_KEY,
      `anthropic-version` = "2023-06-01",
      `anthropic-beta` = "message-batches-2024-09-24",
      `Content-Type` = "application/json"
    ) |>
    req_body_json(list(requests = requests)) |>
    req_timeout(300) |>
    req_perform()
  
  result <- resp_body_json(response)
  
  message(sprintf("Batch submitted! ID: %s", result$id))
  message(sprintf("Status: %s", result$processing_status))
  
  return(result)
}

# Check batch status
check_batch_status <- function(batch_id) {
  response <- request(paste0(BATCH_API_URL, "/", batch_id)) |>
    req_headers(
      `x-api-key` = ANTHROPIC_API_KEY,
      `anthropic-version` = "2023-06-01",
      `anthropic-beta` = "message-batches-2024-09-24"
    ) |>
    req_perform()
  
  result <- resp_body_json(response)
  
  message(sprintf("Batch ID: %s", result$id))
  message(sprintf("Status: %s", result$processing_status))
  
  if (!is.null(result$request_counts)) {
    message(sprintf("Progress: %d/%d completed", 
                    result$request_counts$succeeded + result$request_counts$errored,
                    result$request_counts$processing + result$request_counts$succeeded + 
                      result$request_counts$errored + result$request_counts$canceled))
  }
  
  return(result)
}

# Wait for batch completion with polling
wait_for_batch <- function(batch_id, poll_interval = 60) {
  message("Waiting for batch to complete...")
  
  while (TRUE) {
    status <- check_batch_status(batch_id)
    
    if (status$processing_status == "ended") {
      message("Batch completed!")
      return(status)
    }
    
    message(sprintf("Still processing... checking again in %d seconds", poll_interval))
    Sys.sleep(poll_interval)
  }
}

# Download batch results
download_batch_results <- function(batch_id, output_file = "batch_results.jsonl") {
  
  # Get batch info to find results URL
  status <- check_batch_status(batch_id)
  
  if (status$processing_status != "ended") {
    stop("Batch is not yet complete. Current status: ", status$processing_status)
  }
  
  # Download results
  results_url <- status$results_url
  
  response <- request(results_url) |>
    req_headers(
      `x-api-key` = ANTHROPIC_API_KEY,
      `anthropic-version` = "2023-06-01",
      `anthropic-beta` = "message-batches-2024-09-24"
    ) |>
    req_perform()
  
  # Save to file
  writeBin(resp_body_raw(response), output_file)
  
  message(sprintf("Results saved to: %s", output_file))
  return(output_file)
}

# Parse batch results into dataframe
parse_batch_results <- function(results_file) {
  lines <- readLines(results_file)
  
  all_recommendations <- list()
  
  for (line in lines) {
    result <- fromJSON(line, flatten = FALSE)
    
    if (result$result$type == "succeeded") {
      # Extract the text content
      content <- result$result$message$content[[1]]$text
      
      # Clean JSON if needed
      if (grepl("```json", content)) {
        content <- str_extract(content, "(?<=```json\\n)[\\s\\S]*(?=\\n```)")
      } else if (grepl("```", content)) {
        content <- str_extract(content, "(?<=```\\n)[\\s\\S]*(?=\\n```)")
      }
      
      # Parse JSON
      tryCatch({
        parsed <- fromJSON(content, flatten = FALSE)
        
        if (!is.null(parsed$recommendations) && length(parsed$recommendations) > 0) {
          recs <- parsed$recommendations
          
          # Handle if recs is a list vs dataframe
          if (is.data.frame(recs)) {
            recs$doc_id <- parsed$doc_id
            recs$org_name <- parsed$org_name
            recs$org_type <- parsed$org_type
          } else {
            recs <- lapply(recs, function(r) {
              r$doc_id <- parsed$doc_id
              r$org_name <- parsed$org_name
              r$org_type <- parsed$org_type
              return(r)
            })
            recs <- bind_rows(recs)
          }
          
          all_recommendations[[result$custom_id]] <- recs
        }
      }, error = function(e) {
        message(sprintf("Error parsing result for %s: %s", result$custom_id, e$message))
      })
    } else {
      message(sprintf("Request %s failed: %s", result$custom_id, result$result$type))
    }
  }
  
  # Combine all results
  recommendations_df <- bind_rows(all_recommendations)
  
  message(sprintf("Parsed %d recommendations from %d documents", 
                  nrow(recommendations_df), length(all_recommendations)))
  
  return(recommendations_df)
}

# =============================================================================
# METHOD 2: PARALLEL PROCESSING WITH CRASH RECOVERY
# =============================================================================

# Single API call function
call_claude_api <- function(prompt) {
  response <- request("https://api.anthropic.com/v1/messages") |>
    req_headers(
      `x-api-key` = ANTHROPIC_API_KEY,
      `anthropic-version` = "2023-06-01",
      `content-type` = "application/json"
    ) |>
    req_body_json(list(
      model = MODEL,
      max_tokens = 16000,
      messages = list(
        list(role = "user", content = prompt)
      )
    )) |>
    req_timeout(300) |>
    req_retry(max_tries = 3, backoff = ~ 10) |>
    req_perform()
  
  result <- resp_body_json(response)
  return(result$content[[1]]$text)
}

# Extract recommendations for a single document (with error handling)
extract_single_safe <- function(doc_id, org_name, text) {
  tryCatch({
    prompt <- create_extraction_prompt(doc_id, org_name, text)
    response_text <- call_claude_api(prompt)
    
    # Clean JSON if wrapped in markdown
    if (grepl("```json", response_text)) {
      response_text <- str_extract(response_text, "(?<=```json\\n)[\\s\\S]*(?=\\n```)")
    } else if (grepl("```", response_text)) {
      response_text <- str_extract(response_text, "(?<=```\\n)[\\s\\S]*(?=\\n```)")
    }
    
    result <- fromJSON(response_text, flatten = FALSE)
    
    # Add metadata to recommendations
    if (!is.null(result$recommendations)) {
      recs <- result$recommendations
      if (is.data.frame(recs)) {
        recs$doc_id <- result$doc_id
        recs$org_name <- result$org_name
        recs$org_type <- result$org_type
      } else {
        recs <- lapply(recs, function(r) {
          r$doc_id <- result$doc_id
          r$org_name <- result$org_name
          r$org_type <- result$org_type
          return(r)
        })
        recs <- bind_rows(recs)
      }
      return(recs)
    }
    return(NULL)
  }, error = function(e) {
    write_progress(sprintf("❌ Error processing %s: %s", doc_id, e$message))
    return(NULL)
  })
}

# Main processing function with crash recovery
process_with_recovery <- function(documents, n_workers = 4, batch_size = 10) {
  
  # Initialize progress tracking
  init_progress_tracking()
  
  write_progress("🚀 Starting RFI Recommendation Extraction with Crash Recovery")
  write_progress(sprintf("📊 Total documents to process: %d", nrow(documents)))
  write_progress(sprintf("⚙️ Workers: %d | Batch size: %d", n_workers, batch_size))
  
  # Check for previously completed documents
  completed_doc_ids <- get_completed_doc_ids()
  write_progress(sprintf("✅ Already completed: %d documents", length(completed_doc_ids)))
  
  # Filter to remaining documents
  remaining_docs <- documents |>
    filter(!doc_id %in% completed_doc_ids)
  
  if (nrow(remaining_docs) == 0) {
    write_progress("🎉 All documents already completed!")
    return(load_all_saved_batches())
  }
  
  write_progress(sprintf("📝 Remaining to process: %d documents", nrow(remaining_docs)))
  
  # Set up parallel processing
  plan(multisession, workers = n_workers)
  write_progress(sprintf("🔧 Parallel processing set up with %d workers", future::nbrOfWorkers()))
  
  # Split into batches
  n_batches <- ceiling(nrow(remaining_docs) / batch_size)
  doc_batches <- split(remaining_docs, ceiling(seq_len(nrow(remaining_docs)) / batch_size))
  
  write_progress(sprintf("📦 Processing %d batches of up to %d documents each", n_batches, batch_size))
  
  # Process each batch
  for (batch_num in seq_along(doc_batches)) {
    batch <- doc_batches[[batch_num]]
    
    write_progress(sprintf("\n=== BATCH %d/%d ===", batch_num, n_batches))
    write_progress(sprintf("📄 Documents in batch: %d", nrow(batch)))
    write_progress(sprintf("🏢 Organizations: %s", 
                           paste(head(batch$org_name, 3), collapse = ", ")))
    
    # Overall progress
    total_completed <- length(completed_doc_ids)
    percent_overall <- round((total_completed / nrow(documents)) * 100, 1)
    write_progress(sprintf("📊 Overall progress: %d/%d (%.1f%%)", 
                           total_completed, nrow(documents), percent_overall))
    
    # Process batch in parallel
    batch_start_time <- Sys.time()
    
    batch_results <- future_pmap(
      list(batch$doc_id, batch$org_name, batch$text),
      function(doc_id, org_name, text) {
        extract_single_safe(doc_id, org_name, text)
      },
      .options = furrr_options(seed = TRUE),
      .progress = TRUE
    )
    
    batch_duration <- as.numeric(difftime(Sys.time(), batch_start_time, units = "mins"))
    
    # Remove NULLs
    batch_results <- compact(batch_results)
    
    write_progress(sprintf("✅ Batch %d completed in %.2f minutes", batch_num, batch_duration))
    write_progress(sprintf("📚 Successful extractions: %d/%d", length(batch_results), nrow(batch)))
    
    if (length(batch_results) > 0) {
      # Combine batch data
      combined_batch <- bind_rows(batch_results)
      
      # Count recommendations
      write_progress(sprintf("📝 Recommendations extracted: %d", nrow(combined_batch)))
      
      # Save batch with unique identifier (using first doc_id in batch)
      batch_file <- file.path(PROGRESS_DIR, sprintf("batch_%s.rds", batch$doc_id[1]))
      safe_save(combined_batch, batch_file)
      
      # Update completed doc IDs
      completed_doc_ids <- c(completed_doc_ids, batch$doc_id)
      save_progress_state(completed_doc_ids)
      
      write_progress(sprintf("💾 Batch saved. Total completed: %d documents", length(completed_doc_ids)))
    }
    
    # Memory cleanup
    rm(batch_results)
    if (exists("combined_batch")) rm(combined_batch)
    gc()
    
    # Brief pause between batches
    Sys.sleep(2)
  }
  
  # Reset to sequential processing
  plan(sequential)
  
  # Load and combine all results
  write_progress("\n=== FINALIZING ===")
  all_recommendations <- load_all_saved_batches()
  
  write_progress(sprintf("🎉 COMPLETE! Total recommendations: %d from %d documents",
                         nrow(all_recommendations), 
                         n_distinct(all_recommendations$doc_id)))
  
  return(all_recommendations)
}

# Quick resume function - just loads existing progress and continues
resume_processing <- function(documents, n_workers = 4, batch_size = 10) {
  write_progress("🔄 Resuming processing from last checkpoint...")
  process_with_recovery(documents, n_workers, batch_size)
}

# Combine all saved batches into final output
finalize_results <- function(output_file = "all_recommendations.rds") {
  write_progress("📦 Combining all saved batches into final output...")
  
  all_recommendations <- load_all_saved_batches()
  
  if (!is.null(all_recommendations) && nrow(all_recommendations) > 0) {
    # Save final combined file
    saveRDS(all_recommendations, output_file)
    write_csv(all_recommendations, str_replace(output_file, "\\.rds$", ".csv"))
    
    write_progress(sprintf("✅ Final results saved to: %s", output_file))
    write_progress(sprintf("📊 Total: %d recommendations from %d documents",
                           nrow(all_recommendations),
                           n_distinct(all_recommendations$doc_id)))
    
    return(all_recommendations)
  } else {
    write_progress("⚠️ No results found to combine")
    return(NULL)
  }
}

# Clean up progress files after successful completion
cleanup_progress <- function() {
  write_progress("🧹 Cleaning up progress files...")
  
  if (dir.exists(PROGRESS_DIR)) {
    unlink(PROGRESS_DIR, recursive = TRUE)
    write_progress("✅ Progress directory removed")
  }
}

# =============================================================================
# SUMMARY STATISTICS
# =============================================================================

summarize_results <- function(recommendations_df) {
  cat("\n=== SUMMARY STATISTICS ===\n\n")
  cat(sprintf("Total recommendations: %d\n", nrow(recommendations_df)))
  cat(sprintf("Total documents: %d\n", n_distinct(recommendations_df$doc_id)))
  cat(sprintf("Avg recommendations per document: %.1f\n\n", 
              nrow(recommendations_df) / n_distinct(recommendations_df$doc_id)))
  
  cat("By Organization Type:\n")
  recommendations_df |>
    count(org_type, sort = TRUE) |>
    print()
  
  cat("\nBy Topic (Primary):\n")
  if ("topics" %in% names(recommendations_df)) {
    recommendations_df |>
      unnest(topics) |>
      filter(topic_id == 1) |>
      count(topic, sort = TRUE) |>
      print()
  }
  
  cat("\nHCAI Values (Primary):\n")
  if ("hcai_values" %in% names(recommendations_df)) {
    recommendations_df |>
      unnest(hcai_values) |>
      filter(value_id == 1) |>
      count(value, sort = TRUE) |>
      print()
  }
  
  cat("\nHCAI Properties (Primary):\n")
  if ("hcai_properties" %in% names(recommendations_df)) {
    recommendations_df |>
      unnest(hcai_properties) |>
      filter(property_id == 1) |>
      count(property, sort = TRUE) |>
      print()
  }
  
  cat("\nHCAI Purposes (Primary):\n")
  if ("hcai_purposes" %in% names(recommendations_df)) {
    recommendations_df |>
      unnest(hcai_purposes) |>
      filter(purpose_id == 1) |>
      count(purpose, sort = TRUE) |>
      print()
  }
}

# =============================================================================
# USAGE EXAMPLES
# =============================================================================

# Load your data
# documents <- load_submissions_from_rds("path/to/submissions_full_text.rds")

# -----------------------------------------------------------------------------
# OPTION A: BATCH API (Recommended for 333 documents - 50% cheaper, ~30 min)
# -----------------------------------------------------------------------------

# Step 1: Create batch request file
# create_batch_requests(documents, "batch_requests.jsonl")

# Step 2: Submit batch
# batch <- submit_batch_from_file("batch_requests.jsonl")
# batch_id <- batch$id

# Step 3: Wait for completion (polls every 60 seconds)
# status <- wait_for_batch(batch_id, poll_interval = 60)
# OR check manually:
# status <- check_batch_status(batch_id)

# Step 4: Download results
# download_batch_results(batch_id, "batch_results.jsonl")

# Step 5: Parse results
# recommendations <- parse_batch_results("batch_results.jsonl")

# Step 6: Save final results
# saveRDS(recommendations, "all_recommendations.rds")
# write_csv(recommendations, "all_recommendations.csv")

# -----------------------------------------------------------------------------
# OPTION B: PARALLEL PROCESSING WITH CRASH RECOVERY (Recommended)
# -----------------------------------------------------------------------------

# FIRST RUN: Start fresh processing
# recommendations <- process_with_recovery(
#   documents, 
#   n_workers = 8,    # Adjust based on your CPU cores
#   batch_size = 10   # Save progress every 10 documents
# )

# IF CRASHED: Resume from last checkpoint (just run the same command!)
# recommendations <- process_with_recovery(
#   documents, 
#   n_workers = 8,
#   batch_size = 10
# )
# It will automatically detect completed documents and skip them!

# OR use the explicit resume function:
# recommendations <- resume_processing(documents, n_workers = 8, batch_size = 10)

# AFTER COMPLETION: Combine all batches into final file
# recommendations <- finalize_results("all_recommendations.rds")

# OPTIONAL: Clean up temporary progress files
# cleanup_progress()

# View summary
# summarize_results(recommendations)

# -----------------------------------------------------------------------------
# QUICK START (Copy-paste this block)
# -----------------------------------------------------------------------------

# # Load data
# documents <- load_submissions_from_rds("C:/Users/luzya/OneDrive/Escritorio/DGO_2026/data/submissions_full_text.rds")
# 
# # Process with crash recovery (will resume if interrupted)
# recommendations <- process_with_recovery(
#   documents,
#   n_workers = 8,
#   batch_size = 10
# )
# 
# # Save final results
# saveRDS(recommendations, "all_recommendations.rds")
# write_csv(recommendations, "all_recommendations.csv")
# 
# # View summary
# summarize_results(recommendations)