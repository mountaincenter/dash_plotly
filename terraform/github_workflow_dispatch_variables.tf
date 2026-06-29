variable "github_dispatch_owner" {
  description = "GitHub repository owner for scheduled workflow dispatch"
  type        = string
  default     = "mountaincenter"
}

variable "github_dispatch_repo" {
  description = "GitHub repository name for scheduled workflow dispatch"
  type        = string
  default     = "dash_plotly"
}

variable "github_dispatch_workflow_id" {
  description = "GitHub Actions workflow file name or workflow id to dispatch"
  type        = string
  default     = "data-pipeline.yml"
}

variable "github_dispatch_ref" {
  description = "Git ref used for scheduled workflow_dispatch"
  type        = string
  default     = "main"
}

variable "github_dispatch_token_parameter_name" {
  description = "SSM Parameter Store SecureString name containing the GitHub token"
  type        = string
  default     = "/stock-pipeline/github/token"

  validation {
    condition     = startswith(var.github_dispatch_token_parameter_name, "/")
    error_message = "github_dispatch_token_parameter_name must start with '/'."
  }
}

variable "github_dispatch_schedules_enabled" {
  description = "Whether EventBridge Scheduler schedules for GitHub workflow dispatch are enabled"
  type        = bool
  default     = false
}
