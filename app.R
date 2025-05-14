# app.R

library(shiny)
library(rslurm)
library(DT) # For interactive tables
library(dplyr) # For data manipulation
library(lubridate) # For time calculations

# Ensure this R script file is saved as 'app.R' in its own directory.

# --- 1. Dummy function to be executed on the cluster ---
# This function simulates some work with a random sleep time and returns the node name.
my_task_function <- function(param_id) { # Removed duration_sec from parameters
  # Each task sleeps for a random time between 10 and 20 seconds
  sleep_duration <- sample(10:20, 1)
  Sys.sleep(sleep_duration) # Simulate work
  node_name <- Sys.info()[["nodename"]]
  process_id <- Sys.getpid()
  result_value <- param_id * 2 # Simple placeholder result
  # Important: rslurm collects what's RETURNED by the function.
  return(data.frame(
    task_id_returned = param_id,
    node_name = as.character(node_name),
    process_id_on_node = process_id,
    actual_sleep_duration_sec = sleep_duration,
    simulated_result = result_value,
    stringsAsFactors = FALSE
  ))
}

# --- 2. Shiny UI Definition ---
ui <- fluidPage(
  titlePanel("rslurm Job Launcher & Monitor (Random Sleep)"),
  sidebarLayout(
    sidebarPanel(
      numericInput("n_procs", "Number of Processes (Tasks):", value = 2, min = 1, max = 100, step = 1),
      # Removed numericInput for task_duration
      actionButton("start_button", "Start Jobs", icon = icon("play")),
      actionButton("cancel_button", "Cancel Jobs", icon = icon("stop"), class = "btn-danger"),
      hr(),
      p("Each job will sleep for a random time between 10-20 seconds."),
      p("Job status will refresh automatically."),
      p("Ensure your SLURM environment is configured for rslurm.")
    ),
    mainPanel(
      h4("Job Submission Info:"),
      verbatimTextOutput("slurm_job_info"),
      hr(),
      h4("Task Status:"),
      DTOutput("status_table"),
      hr(),
      h4("Job Output/Results (after completion):"),
      verbatimTextOutput("results_info"),
      DTOutput("results_table")
    )
  )
)

# --- 3. Shiny Server Logic ---
server <- function(input, output, session) {

  # Reactive values to store job information and results
  slurm_job_obj <- reactiveVal(NULL) # Stores the object from slurm_apply
  job_params_df <- reactiveVal(NULL) # Stores the parameters used for the job
  job_submission_time <- reactiveVal(NULL) # Stores when the job was submitted

  # --- Job Submission ---
  observeEvent(input$start_button, {
    if (!is.null(slurm_job_obj())) {
        # Check current status before allowing new submission
        sjob_current_check <- slurm_job_obj()
        status_check <- tryCatch(rslurm::get_job_status(sjob_current_check), error = function(e) NULL)
        if (!is.null(status_check) && status_check$job_state %in% c("PENDING", "RUNNING", "COMPLETING")) {
            showNotification("A job is already active (pending, running, or completing). Please wait or cancel it.", type = "warning", duration=7)
            return()
        }
    }


    n <- as.integer(input$n_procs)
    if (is.na(n) || n < 1) {
      showNotification("Please enter a valid number of processes.", type = "error")
      return()
    }

    showNotification(paste("Submitting", n, "tasks to SLURM..."), type = "message", duration = 5)

    # Parameters for each task (only param_id is needed now)
    params <- data.frame(
        param_id = 1:n
        # duration_sec removed
    )
    job_params_df(params) # Store for later use in status table

    # SLURM options
    current_time_str <- format(Sys.time(), "%Y%m%d_%H%M%S")
    jobname <- paste0("shiny_slurm_rand_", current_time_str)

    sjob <- tryCatch({
      rslurm::slurm_apply(
        my_task_function,
        params = params,
        jobname = jobname,
        nodes = n,
        cpus_per_node = 1,
        pkgs = c("dplyr"),
        slurm_options = list(
          time = "0-00:10:00", # 10 minutes timeout for the whole job array
          'mail-type' = 'FAIL',
          array = paste0("0-", n - 1)
        ),
        submit = TRUE
      )
    }, error = function(e) {
      showNotification(paste("SLURM submission failed:", e$message), type = "error", duration = 10)
      NULL
    })

    if (!is.null(sjob)) {
      slurm_job_obj(sjob)
      job_submission_time(Sys.time())
      showNotification(paste("Job submitted. SLURM Job ID:", sjob$jobid), type = "success", duration = 5)
      output$results_info <- renderPrint({ "Results will appear here after jobs complete." })
      output$results_table <- renderDT(NULL) # Clear previous results table
    } else {
      slurm_job_obj(NULL)
      job_submission_time(NULL)
      job_params_df(NULL)
    }
  })

  # --- Job Cancellation ---
  observeEvent(input$cancel_button, {
    sjob <- slurm_job_obj()
    if (is.null(sjob)) {
      showNotification("No active job to cancel.", type = "warning")
      return()
    }
    tryCatch({
      rslurm::cancel_slurm(sjob)
      showNotification(paste("Cancel request sent for SLURM Job ID:", sjob$jobid), type = "info")
    }, error = function(e) {
      showNotification(paste("Failed to cancel job:", e$message), type = "error")
    })
  })

  # --- Display Submitted Job Info ---
  output$slurm_job_info <- renderPrint({
    sjob <- slurm_job_obj()
    if (is.null(sjob)) {
      "No job submitted yet."
    } else {
      paste0(
        "Slurm Job ID: ", sjob$jobid, "\n",
        "Number of tasks: ", nrow(sjob$job_params), "\n",
        "Temporary files in: _rslurm_", sjob$jobname
      )
    }
  })

  # --- Poll for Job Status and Results ---
  job_status_data <- reactivePoll(
    intervalMillis = 5000, # Poll every 5 seconds
    session = session,
    checkFunc = function() {
      sjob_current <- slurm_job_obj()
      if (!is.null(sjob_current)) {
        # Always check if there's a job object.
        # The valueFunc will decide if it's truly active or terminal.
        return(paste(sjob_current$jobid, Sys.time())) # Force update if job object exists
      }
      return(NULL)
    },
    valueFunc = function() {
      sjob <- slurm_job_obj()
      if (is.null(sjob) || is.null(job_params_df())) { # Check job_params_df as well
        return(data.frame(
          Task = integer(0), SlurmTaskID = character(0), Status = character(0),
          Node = character(0), Runtime = character(0),
          stringsAsFactors = FALSE
        ))
      }

      num_tasks <- nrow(job_params_df())
      base_df <- data.frame(
        Task = 1:num_tasks,
        SlurmTaskID = paste0(sjob$jobid, "_", 0:(num_tasks - 1)),
        Status = rep("FETCHING", num_tasks), # Initial status while querying
        Node = rep("N/A", num_tasks),
        Runtime = rep("N/A", num_tasks),
        stringsAsFactors = FALSE
      )

      job_status_list <- tryCatch({ # Renamed to avoid conflict
        rslurm::get_job_status(sjob)
      }, error = function(e) {
        message(paste("Error polling job status for", sjob$jobid, ":", e$message))
        NULL
      })

      if (is.null(job_status_list)) {
        base_df$Status <- "COULD_NOT_QUERY"
        return(base_df)
      }
      
      current_slurm_job_state <- job_status_list$job_state # This is the overall job state

      # Update status for all tasks
      if (length(current_slurm_job_state) == 1) {
        base_df$Status <- current_slurm_job_state
      } else if (length(current_slurm_job_state) == num_tasks) {
        # This case is less common for get_job_status for job arrays unless specific parsing is done by rslurm
        base_df$Status <- current_slurm_job_state
      } else {
         # If the job_state is complex (e.g. "MIXED" or specific per-task states from more detailed squeue parsing not directly exposed)
         # We rely on the overall state for now. Individual task states might appear more clearly
         # in sacct_output if rslurm provides it, or after get_slurm_out.
         base_df$Status <- current_slurm_job_state[1] # Use the primary reported state
         if (length(current_slurm_job_state) > 1) { # Potentially MIXED or similar
             # You might want to show "MIXED" or parse further if rslurm provided more detail here
             # For now, using the first reported state as a general indicator.
         }
      }

      # Runtime estimation
      submission_time <- job_submission_time()
      if (!is.null(submission_time)) {
           elapsed_seconds <- round(as.numeric(difftime(Sys.time(), submission_time, units = "secs")))
           formatted_runtime <- format(lubridate::seconds_to_period(elapsed_seconds), "%H:%M:%S")
           if (any(base_df$Status == "RUNNING")) {
                base_df$Runtime[base_df$Status == "RUNNING"] <- formatted_runtime
           }
           # More precise runtime for completed tasks comes from sacct (via get_job_status or get_slurm_out)
           if (current_slurm_job_state == "COMPLETED" && !is.null(job_status_list$sacct_output)) {
                sacct_df <- job_status_list$sacct_output
                if (nrow(sacct_df) > 0 && "Elapsed" %in% names(sacct_df)) {
                    # Try to map sacct output to tasks. This can be tricky.
                    # Simplistic approach: if one "Elapsed" time, apply to completed tasks.
                    # A robust solution would match TaskID from sacct_df if available.
                    # For now, this part is illustrative.
                    # base_df$Runtime[base_df$Status == "COMPLETED"] <- sacct_df$Elapsed[1] # Example
                }
           }
      }

      terminal_states <- c("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "OUT_OF_MEMORY")
      # Check if the *overall* job state is terminal, or if all individual task statuses are terminal
      is_job_terminal <- current_slurm_job_state %in% terminal_states

      if (is_job_terminal) {
        showNotification("Overall job state is terminal. Attempting to fetch results...", type = "message", duration = 3)
        results <- tryCatch({
          rslurm::get_slurm_out(sjob, outtype = "table", wait = FALSE) # wait=FALSE as job status says it's done
        }, error = function(e) {
          message(paste("Error fetching results for", sjob$jobid, ":", e$message))
          NULL
        })

        if (!is.null(results) && nrow(results) > 0) {
          output$results_info <- renderPrint({ paste("Results for SLURM Job ID:", sjob$jobid) })
          output$results_table <- renderDT({
              datatable(results, options = list(scrollX = TRUE, pageLength = min(5, nrow(results)+1) )) # Adjust pageLength
          })

          results_to_merge <- results[, c("task_id_returned", "node_name", "actual_sleep_duration_sec")]
          names(results_to_merge)[names(results_to_merge) == "task_id_returned"] <- "Task"
          
          # Ensure 'Task' column is of the same type for merging
          base_df$Task <- as.integer(base_df$Task)
          results_to_merge$Task <- as.integer(results_to_merge$Task)

          base_df <- base_df %>%
            left_join(results_to_merge, by = "Task") %>%
            mutate(
              Node = ifelse(!is.na(node_name.y), node_name.y, Node),
              Status = ifelse(Status == "COMPLETED" & !is.na(node_name.y), "COMPLETED (Results OK)", Status),
              Runtime = ifelse(!is.na(actual_sleep_duration_sec), paste(actual_sleep_duration_sec, "s (actual)"), Runtime)
            ) %>%
            select(-ends_with(".y"), -ends_with(".x"), -any_of("actual_sleep_duration_sec")) # Clean up

        } else if (!is.null(results) && nrow(results) == 0 && current_slurm_job_state == "COMPLETED") {
            output$results_info <- renderPrint({paste("Job ID:", sjob$jobid, "- tasks reported COMPLETED by SLURM but get_slurm_out returned empty or no results. Check _rslurm logs.")})
        } else if (is.null(results) && any(base_df$Status %in% c("FAILED", "TIMEOUT", "CANCELLED"))) { # Check base_df status too
            output$results_info <- renderPrint({paste("Job ID:", sjob$jobid, "- one or more tasks failed or were cancelled. No results table expected or results fetch failed. Check _rslurm logs.")})
        }
      }
      return(base_df)
    }
  )

  # --- Render Status Table ---
  output$status_table <- renderDT({
    df_to_display <- job_status_data()
    validate(
      need(nrow(df_to_display) > 0 || is.null(slurm_job_obj()), "No tasks submitted or status pending first fetch.")
    )
    datatable(df_to_display,
              options = list(scrollX = TRUE, pageLength = 10, autoWidth = TRUE),
              rownames = FALSE,
              caption = "Live status of submitted tasks. Node and precise Runtime are best shown after task completion.")
  })

  # --- Cleanup on session end ---
  session$onSessionEnded(function() {
    sjob <- slurm_job_obj()
    if (!is.null(sjob)) {
      message(paste0(
        "Shiny session ended. SLURM job ", sjob$jobid,
        " (associated with _rslurm_", sjob$jobname, ")",
        " may still be running or files may need manual cleanup if not automatically handled."
      ))
      # Consider if automatic cancellation or cleanup is desired here.
      # For example, if job is still PENDING or RUNNING:
      # current_status_on_end <- tryCatch(rslurm::get_job_status(sjob), error = function(e) NULL)
      # if (!is.null(current_status_on_end) && current_status_on_end$job_state %in% c("PENDING", "RUNNING")) {
      #   # rslurm::cancel_slurm(sjob) # Uncomment to cancel on session end
      #   message(paste("Job", sjob$jobid, "was active and might need manual cancellation/cleanup."))
      # }
    }
  })
}

# --- 4. Run the Shiny App ---
shinyApp(ui = ui, server = server)
