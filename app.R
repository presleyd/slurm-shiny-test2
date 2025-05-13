library(shiny)
library(rslurm)

# User Interface (UI)
ui <- fluidPage(
  titlePanel("Launch and Monitor Slurm Jobs"),
  sidebarLayout(
    sidebarPanel(
      numericInput("num_processes", "Number of Processes (1-10):", min = 1, max = 10, value = 3),
      actionButton("launch_jobs", "Launch Jobs")
    ),
    mainPanel(
      h4("Job Status:"),
      verbatimTextOutput("job_status"),
      h4("Node Information:"),
      verbatimTextOutput("node_info")
    )
  )
)

# Server Logic
server <- function(input, output, session) {
  job_ids <- reactiveVal(NULL)
  job_info <- reactiveVal(data.frame(job_id = character(0), name = character(0), status = character(0), node = character(0), stringsAsFactors = FALSE))

  observeEvent(input$launch_jobs, {
    num_processes <- input$num_processes
    job_names <- paste0("shiny_job_", 1:num_processes)

    slurm_tasks <- list()
    for (i in 1:num_processes) {
      sleep_time <- sample(10:30, 1)
      script <- sprintf("sleep %d && hostname > output_%d.txt", sleep_time, i)
      slurm_tasks[[i]] <- slurm_call(
        function() system(sprintf("%s", script)),
        jobname = job_names[i],
        output = paste0("slurm-%j.out"),
        error = paste0("slurm-%j.err")
      )
    }

    submitted_jobs <- slurm_map(slurm_tasks)
    if (!is.null(submitted_jobs) && !is.null(submitted_jobs$job_id)) {
      job_ids(submitted_jobs$job_id)
      initial_job_info <- data.frame(job_id = submitted_jobs$job_id, name = job_names, status = "PENDING", node = NA, stringsAsFactors = FALSE)
      job_info(initial_job_info)
    }
  })

  output$job_status <- renderPrint({
    job_ids_val <- job_ids()
    if (is.null(job_ids_val) || length(job_ids_val) == 0) {
      return("")
    }

    status_data <- tryCatch({
      squeue(job = job_ids_val, state = "all", o = "jobid,name,state")
    }, error = function(e) {
      NULL
    })

    if (!is.null(status_data) && nrow(status_data) > 0) {
      updated_info <- job_info()
      for (i in 1:nrow(updated_info)) {
        match_row <- which(status_data$JOBID == updated_info$job_id[i])
        if (length(match_row) > 0) {
          updated_info$status[i] <- as.character(status_data$ST[match_row])
        }
      }
      job_info(updated_info)
      print(job_info()[, c("name", "status")])
    } else if (!is.null(status_data)) {
      print("No jobs running or pending.")
    } else {
      print("Error retrieving job status from Slurm.")
    }
  })

  output$node_info <- renderPrint({
    job_ids_val <- job_ids()
    if (is.null(job_ids_val) || length(job_ids_val) == 0) {
      return("")
    }

    node_data <- tryCatch({
      squeue(job = job_ids_val, state = "RUNNING", o = "jobid,nodelist")
    }, error = function(e) {
      NULL
    })

    if (!is.null(node_data) && nrow(node_data) > 0) {
      updated_info <- job_info()
      for (i in 1:nrow(updated_info)) {
        match_row <- which(node_data$JOBID == updated_info$job_id[i])
        if (length(match_row) > 0) {
          updated_info$node[i] <- as.character(node_data$NODELIST[match_row])
        }
      }
      job_info(updated_info)
      print(job_info()[!is.na(job_info()$node), c("name", "node")])
    } else if (!is.null(node_data)) {
      print("No jobs currently running to display node information.")
    } else {
      print("Error retrieving node information from Slurm.")
    }
  })

  # Periodically update job status and node info
  observe({
    invalidateLater(5000) # Update every 5 seconds
    output$job_status
    output$node_info
  })
}

# Run the application
shinyApp(ui = ui, server = server)
