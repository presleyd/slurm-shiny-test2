library(shiny)
library(slurmr)

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

server <- function(input, output, session) {
  job_ids <- reactiveVal(NULL)
  job_info <- reactiveVal(NULL)

  observeEvent(input$launch_jobs, {
    num_processes <- input$num_processes
    job_names <- paste0("shiny_job_", 1:num_processes)

    # Create Slurm tasks
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

    # Submit the Slurm jobs
    submitted_jobs <- slurm_map(slurm_tasks)
    job_ids(submitted_jobs$job_id)
    job_info(data.frame(job_id = submitted_jobs$job_id, name = job_names, status = "PENDING", node = NA))
  })

  output$job_status <- renderPrint({
    current_job_ids <- job_ids()
    if (!is.null(current_job_ids)) {
      status <- squeue(job = current_job_ids, state = "all", o = "jobid,name,state")
      if (nrow(status) > 0) {
        updated_job_info <- job_info()
        for (i in 1:nrow(updated_job_info)) {
          job_match <- which(status$JOBID == updated_job_info$job_id[i])
          if (length(job_match) > 0) {
            updated_job_info$status[i] <- as.character(status$ST[job_match])
          }
        }
        job_info(updated_job_info)
        print(updated_job_info[, c("name", "status")])
      } else {
        print("No jobs running or pending.")
      }
    } else {
      print("No jobs launched yet.")
    }
  })

  output$node_info <- renderPrint({
    current_job_ids <- job_ids()
    if (!is.null(current_job_ids)) {
      node_data <- squeue(job = current_job_ids, state = "RUNNING", o = "jobid,nodelist")
      if (nrow(node_data) > 0) {
        updated_job_info <- job_info()
        for (i in 1:nrow(updated_job_info)) {
          node_match <- which(node_data$JOBID == updated_job_info$job_id[i])
          if (length(node_match) > 0) {
            updated_job_info$node[i] <- as.character(node_data$NODELIST[node_match])
          }
        }
        job_info(updated_job_info)
        print(job_info()[!is.na(job_info()$node), c("name", "node")])
      } else {
        print("No jobs currently running to display node information.")
      }
    } else {
      print("No jobs launched yet.")
    }
  })

  # Periodically update job status and node info
  observe({
    invalidateLater(5000) # Update every 5 seconds
    output$job_status
    output$node_info
  })
}

shinyApp(ui = ui, server = server)
