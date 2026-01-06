# AWS MWAA Environment Context for AI Analysis

**Instructions for AI**: This file provides context about the AWS MWAA environment. Use this information to set your analysis context, then examine all provided files to identify issues, performance problems, and best practices violations. Look beyond just the symptoms listed - analyze logs, DAGs, and configurations comprehensively.

## Environment Details

### MWAA Environment
- **Environment Name**: `[e.g., my-mwaa-env, production-airflow]`
- **MWAA Version**: `[e.g., 2.8.1, 2.7.3, 2.9.0]`
- **Environment Class**: `[Choose: mw1.small, mw1.medium, mw1.large, mw1.xlarge, mw1.2xlarge]`
- **Region**: `[e.g., us-east-1, us-west-2, eu-west-1]`

### Current Configuration
- **Min Workers**: `[e.g., 1, 2, 5]`
- **Max Workers**: `[e.g., 10, 25, 50]`
- **Scheduler Count**: `[e.g., 2, 4, 8]` (only for medium+ environments)

## Problem Overview

### When Did Issues Start
`[e.g., 3 days ago, after last deployment, December 20th]`

### Symptoms Observed
Check all that apply - **AI should look for additional issues beyond these**:
- [ ] DAG import timeouts
- [ ] Tasks stuck in queued state  
- [ ] Worker crashes/restarts
- [ ] Scheduler performance issues
- [ ] Memory/CPU exhaustion
- [ ] Task execution failures
- [ ] Zombie/orphaned tasks
- [ ] Environment updates failing
- [ ] UI performance issues

### Affected DAGs
- **Primary DAG(s)**: `[e.g., etl_pipeline, data_sync_dag]`
- **DAG Characteristics**: `[e.g., dynamic generation, heavy I/O, complex dependencies]`

## Recent Changes

### Last Environment Update
- **Date**: `[e.g., 2024-01-05, no recent updates]`
- **What Changed**: `[e.g., added new DAGs, updated requirements.txt, changed environment class]`

### Recent DAG Changes
- [ ] New DAGs added
- [ ] Existing DAGs modified  
- [ ] Dependencies updated
- [ ] Configuration changes
- [ ] No recent changes

**Details**: `[Brief description of what changed]`

## Files Provided

### DAG Files
- [ ] Problematic DAG files included
- [ ] Supporting modules included
- [ ] All relevant DAGs included

### Logs  
- [ ] CloudWatch scheduler logs
- [ ] CloudWatch worker logs
- [ ] CloudWatch webserver logs
- [ ] Task execution logs

### Configuration
- [ ] Custom airflow.cfg overrides (if any)
- [ ] requirements.txt
- [ ] Other configuration files

### Support Files
- [ ] Timeline or context information
- [ ] Performance data
- [ ] Previous troubleshooting notes

---

## AI Analysis Instructions

Please perform a comprehensive analysis focusing on:

### 1. Root Cause Analysis
- Examine logs for error patterns and performance issues
- Analyze DAG code for best practices violations
- Check for resource constraints and bottlenecks
- Identify configuration problems

### 2. Performance Assessment  
- DAG parsing performance
- Task execution efficiency
- Resource utilization patterns
- Scaling issues

### 3. Best Practices Review
- Code quality and structure
- Configuration optimization
- Architecture patterns
- Security considerations

### 4. Specific MWAA Issues
- Environment sizing appropriateness
- Worker scaling patterns
- CloudWatch log analysis
- MWAA-specific limitations

### 5. Recommendations
- Immediate fixes for critical issues
- Configuration optimizations
- Code refactoring suggestions
- Long-term architectural improvements

**Priority**: Focus on issues that cause the most business impact first, then address performance optimizations and preventive measures.

---

**Completed By**: `[Your name/team]`
**Date**: Add todays date.