import { FastifyReply, FastifyRequest } from "fastify";
import { createApiResponse, getDriveOwnerId, OrgIdParams } from "../../types";
import {
  DriveID,
  GenerateID,
  IDPrefixEnum,
  IPaginatedResponse,
  IRequestCreateJobRun,
  IRequestDeleteJobRun,
  IRequestGetJobRun,
  IRequestListJobRuns,
  IRequestUpdateJobRun,
  IResponseDeleteJobRun,
  JobRun,
  JobRunFE,
  JobRunStatus,
  SortDirection,
  SystemPermissionType,
  SystemResourceID,
  SystemTableValueEnum,
} from "@officexapp/types";
import { authenticateRequest } from "../../../../services/auth";
import { db, dbHelpers } from "../../../../services/database";
import {
  validateDescription,
  validateIdString,
  validateShortString,
  validateUrl,
} from "../../../../services/validation";
import {
  claimUUID,
  isUUIDClaimed,
  updateExternalIDMapping,
} from "../../../../services/external";
import {
  checkSystemPermissions as checkSystemPermissionsService,
  canUserAccessSystemPermission as canUserAccessSystemPermissionService,
  redactLabelValue,
} from "../../../../services/permissions/system";
import { GetJobRunParams } from ".";

export async function getJobRunHandler(
  request: FastifyRequest<{ Params: GetJobRunParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id, job_run_id: jobRunId } = request.params;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const jobRuns = await db.queryDrive(
      org_id,
      "SELECT * FROM job_runs WHERE id = ?",
      [jobRunId]
    );

    if (!jobRuns || jobRuns.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "JobRun not found",
        })
      );
    }

    const jobRun = jobRuns[0] as JobRun;

    const permissionPreviews = isOwner
      ? [
          SystemPermissionType.CREATE,
          SystemPermissionType.EDIT,
          SystemPermissionType.DELETE,
          SystemPermissionType.VIEW,
          SystemPermissionType.INVITE,
        ]
      : await Promise.resolve().then(async () => {
          const recordPermissions = await checkSystemPermissionsService(
            `${jobRunId}` as SystemResourceID,
            requesterApiKey.user_id,
            org_id
          );
          const tablePermissions = await checkSystemPermissionsService(
            `TABLE_${SystemTableValueEnum.JOB_RUNS}` as SystemResourceID, // Assuming JOB_RUNS exists in SystemTableValueEnum
            requesterApiKey.user_id,
            org_id
          );
          return Array.from(
            new Set([...recordPermissions, ...tablePermissions])
          );
        });

    const jobRunFE: JobRunFE = {
      ...jobRun,
      permission_previews: permissionPreviews,
      related_resources: [], // Ensure related_resources is an empty array as it's no longer used
    };

    // Redaction logic based on Rust's JobRunFE::redacted
    const isVendorOfJob = requesterApiKey.user_id === jobRun.vendor_id;
    const hasTableViewPermission = jobRunFE.permission_previews.includes(
      SystemPermissionType.VIEW
    );

    if (!isVendorOfJob && !hasTableViewPermission) {
      jobRunFE.notes = "";
    }
    if (!isVendorOfJob && !hasTableViewPermission) {
      jobRunFE.vendor_notes = "";
      jobRunFE.tracer = undefined;
    }

    const jobRunLabelsRaw = await db.queryDrive(
      org_id,
      `SELECT T2.value FROM job_run_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.job_run_id = ?`,
      [jobRun.id]
    );
    jobRunFE.labels = (
      await Promise.all(
        jobRunLabelsRaw.map((row: any) =>
          redactLabelValue(org_id, row.value, requesterApiKey.user_id)
        )
      )
    ).filter((label): label is string => label !== null);

    // Removed job_run_related_resources query

    return reply.status(200).send(createApiResponse(jobRunFE));
  } catch (error) {
    request.log.error("Error in getJobRunHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function listJobRunsHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestListJobRuns }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const validation = validateListJobRunsRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const hasTablePermission = await checkSystemPermissionsService(
      `TABLE_${SystemTableValueEnum.JOB_RUNS}` as SystemResourceID,
      requesterApiKey.user_id,
      org_id
    ).then((perms) => perms.includes(SystemPermissionType.VIEW));

    if (!isOwner && !hasTablePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    let sql = `SELECT * FROM job_runs`;
    const params: any[] = [];
    const orderBy =
      body.direction === SortDirection.DESC
        ? "created_at DESC"
        : "created_at ASC";
    const pageSize = body.page_size || 50;
    let offset = 0;

    if (body.cursor) {
      offset = parseInt(body.cursor, 10);
      if (isNaN(offset)) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: "Invalid cursor format",
          })
        );
      }
    }

    // TODO: Implement filtering based on `body.filters`
    if (body.filters && body.filters.length > 0) {
      request.log.warn(
        `[TODO: FEATURE] Filtering by '${body.filters}' for JobRuns is not yet implemented.`
      );
    }

    sql += ` ORDER BY ${orderBy} LIMIT ? OFFSET ?`;
    params.push(pageSize + 1, offset);

    const allJobRuns = await db.queryDrive(org_id, sql, params);

    let itemsToReturn: JobRun[] = [];
    let nextCursor: string | null = null;
    let totalCount = 0;

    if (allJobRuns.length > pageSize) {
      nextCursor = (offset + pageSize).toString();
      itemsToReturn = allJobRuns.slice(0, pageSize) as JobRun[];
    } else {
      itemsToReturn = allJobRuns as JobRun[];
    }

    if (isOwner || hasTablePermission) {
      const totalResult = await db.queryDrive(
        org_id,
        "SELECT COUNT(*) as count FROM job_runs"
      );
      totalCount = totalResult[0].count;
    } else {
      totalCount = itemsToReturn.length;
      if (nextCursor) {
        totalCount += 1;
      }
    }

    const redactedJobRuns = await Promise.all(
      itemsToReturn.map(async (jobRun) => {
        const jobRunFE: JobRunFE = {
          ...jobRun,
          labels: [],
          related_resources: [], // Ensure related_resources is an empty array
          permission_previews: isOwner
            ? [
                SystemPermissionType.CREATE,
                SystemPermissionType.EDIT,
                SystemPermissionType.DELETE,
                SystemPermissionType.VIEW,
                SystemPermissionType.INVITE,
              ]
            : await Promise.resolve().then(async () => {
                const recordPermissions = await checkSystemPermissionsService(
                  `${jobRun.id}` as SystemResourceID,
                  requesterApiKey.user_id,
                  org_id
                );
                const tablePermissions = await checkSystemPermissionsService(
                  `TABLE_${SystemTableValueEnum.JOB_RUNS}` as SystemResourceID,
                  requesterApiKey.user_id,
                  org_id
                );
                return Array.from(
                  new Set([...recordPermissions, ...tablePermissions])
                );
              }),
        };

        const isVendorOfJob = requesterApiKey.user_id === jobRun.vendor_id;
        const hasCurrentTableViewPermission =
          jobRunFE.permission_previews.includes(SystemPermissionType.VIEW);

        if (!isVendorOfJob && !hasCurrentTableViewPermission) {
          jobRunFE.notes = "";
        }
        if (!isVendorOfJob && !hasCurrentTableViewPermission) {
          jobRunFE.vendor_notes = "";
          jobRunFE.tracer = undefined;
        }

        const listJobRunLabelsRaw = await db.queryDrive(
          org_id,
          `SELECT T2.value FROM job_run_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.job_run_id = ?`,
          [jobRun.id]
        );
        jobRunFE.labels = (
          await Promise.all(
            listJobRunLabelsRaw.map((row: any) =>
              redactLabelValue(org_id, row.value, requesterApiKey.user_id)
            )
          )
        ).filter((label): label is string => label !== null);

        // Removed job_run_related_resources query

        return jobRunFE;
      })
    );

    const responseData: IPaginatedResponse<JobRunFE> = {
      items: redactedJobRuns,
      page_size: itemsToReturn.length,
      total: totalCount,
      direction: body.direction || SortDirection.ASC,
      cursor: nextCursor || undefined,
    };

    return reply.status(200).send(createApiResponse(responseData));
  } catch (error) {
    request.log.error("Error in listJobRunsHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function createJobRunHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestCreateJobRun }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const validation = await validateCreateJobRunRequest(body, org_id);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const userPermissions = await checkSystemPermissionsService(
      `TABLE_${SystemTableValueEnum.JOB_RUNS}` as SystemResourceID,
      requesterApiKey.user_id,
      org_id
    );

    const hasCreatePermission =
      isOwner || userPermissions.includes(SystemPermissionType.CREATE);

    if (!hasCreatePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const jobRunId = body.id || GenerateID.JobRunID();
    const now = Date.now();

    const newJobRun: JobRun = await dbHelpers.transaction(
      "drive",
      org_id,
      (database) => {
        const insertJobRunStmt = database.prepare(
          `INSERT INTO job_runs (id, template_id, vendor_name, vendor_id, status, description, about_url, billing_url, support_url, delivery_url, verification_url, installation_url, title, subtitle, pricing, vendor_notes, notes, created_at, updated_at, last_updated_at, tracer, external_id, external_payload)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        );
        insertJobRunStmt.run(
          jobRunId,
          body.template_id || null,
          body.vendor_name,
          body.vendor_id,
          body.status || JobRunStatus.REQUESTED, // Default status if not provided
          body.description || null,
          body.about_url || "",
          body.billing_url || null,
          body.support_url || null,
          body.delivery_url || null,
          body.verification_url || null,
          body.auth_installation_url || null,
          body.title,
          body.subtitle || null,
          body.pricing || null,
          body.vendor_notes || null,
          body.notes || null,
          now,
          now,
          now,
          body.tracer || null,
          body.external_id || null,
          body.external_payload || null
        );

        if (body.labels && body.labels.length > 0) {
          const insertLabelStmt = database.prepare(
            `INSERT INTO job_run_labels (job_run_id, label_id) VALUES (?, ?)`
          );
          for (const labelId of body.labels) {
            insertLabelStmt.run(jobRunId, labelId);
          }
        }

        const createdJobRun: JobRun = {
          id: jobRunId,
          template_id: body.template_id,
          vendor_name: body.vendor_name || "",
          vendor_id: body.vendor_id || "",
          status: body.status || JobRunStatus.REQUESTED,
          description: body.description || "",
          about_url: body.about_url || "",
          billing_url: body.billing_url || "",
          support_url: body.support_url || "",
          delivery_url: body.delivery_url || "",
          verification_url: body.verification_url || "",
          auth_installation_url: body.auth_installation_url || "",
          title: body.title || "",
          subtitle: body.subtitle || "",
          pricing: body.pricing || "",
          next_delivery_date: body.next_delivery_date || -1,
          vendor_notes: body.vendor_notes || "",
          notes: body.notes || "",
          created_at: now,
          updated_at: now,
          last_updated_at: now,
          labels: body.labels || [],
          related_resources: [], // Ensure related_resources is empty
          tracer: body.tracer,
          external_id: body.external_id,
          external_payload: body.external_payload,
        };
        return createdJobRun;
      }
    );

    await updateExternalIDMapping(
      org_id,
      undefined,
      newJobRun.external_id,
      newJobRun.id
    );

    if (!body.id) {
      await claimUUID(org_id, newJobRun.id);
    }

    const permissionPreviews = isOwner
      ? [
          SystemPermissionType.CREATE,
          SystemPermissionType.EDIT,
          SystemPermissionType.DELETE,
          SystemPermissionType.VIEW,
          SystemPermissionType.INVITE,
        ]
      : await Promise.resolve().then(async () => {
          const recordPermissions = await checkSystemPermissionsService(
            `${jobRunId}` as SystemResourceID,
            requesterApiKey.user_id,
            org_id
          );
          const tablePermissions = await checkSystemPermissionsService(
            `TABLE_${SystemTableValueEnum.JOB_RUNS}` as SystemResourceID,
            requesterApiKey.user_id,
            org_id
          );
          return Array.from(
            new Set([...recordPermissions, ...tablePermissions])
          );
        });

    const jobRunFE: JobRunFE = {
      ...newJobRun,
      permission_previews: permissionPreviews,
    };

    const isVendorOfJob = requesterApiKey.user_id === newJobRun.vendor_id;
    const hasTableViewPermission = jobRunFE.permission_previews.includes(
      SystemPermissionType.VIEW
    );

    if (!isVendorOfJob && !hasTableViewPermission) {
      jobRunFE.notes = "";
    }
    if (!isVendorOfJob && !hasTableViewPermission) {
      jobRunFE.vendor_notes = "";
      jobRunFE.tracer = undefined;
    }

    const createJobRunLabelsRaw = await db.queryDrive(
      org_id,
      `SELECT T2.value FROM job_run_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.job_run_id = ?`,
      [newJobRun.id]
    );
    jobRunFE.labels = (
      await Promise.all(
        createJobRunLabelsRaw.map((row: any) =>
          redactLabelValue(org_id, row.value, requesterApiKey.user_id)
        )
      )
    ).filter((label): label is string => label !== null);

    jobRunFE.related_resources = []; // Ensure related_resources is empty

    return reply.status(200).send(createApiResponse(jobRunFE));
  } catch (error) {
    request.log.error("Error in createJobRunHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function updateJobRunHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestUpdateJobRun }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const validation = await validateUpdateJobRunRequest(body, org_id);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const jobRunId = body.id;

    const existingJobRuns = await db.queryDrive(
      org_id,
      "SELECT * FROM job_runs WHERE id = ?",
      [jobRunId]
    );

    if (!existingJobRuns || existingJobRuns.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "JobRun not found",
        })
      );
    }
    const existingJobRun = existingJobRuns[0] as JobRun;

    const hasEditPermission = await Promise.resolve().then(async () => {
      const recordPermissions = await checkSystemPermissionsService(
        `${jobRunId}` as SystemResourceID,
        requesterApiKey.user_id,
        org_id
      );
      const tablePermissions = await checkSystemPermissionsService(
        `TABLE_${SystemTableValueEnum.JOB_RUNS}` as SystemResourceID,
        requesterApiKey.user_id,
        org_id
      );
      return (
        recordPermissions.includes(SystemPermissionType.EDIT) ||
        tablePermissions.includes(SystemPermissionType.EDIT)
      );
    });

    if (!isOwner && !hasEditPermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const updates: string[] = [];
    const values: any[] = [];
    const now = Date.now();

    if (body.title !== undefined) {
      updates.push("title = ?");
      values.push(body.title);
    }
    if (body.subtitle !== undefined) {
      updates.push("subtitle = ?");
      values.push(body.subtitle);
    }
    if (body.description !== undefined) {
      updates.push("description = ?");
      values.push(body.description);
    }
    if (body.about_url !== undefined) {
      updates.push("about_url = ?");
      values.push(body.about_url);
    }
    if (body.delivery_url !== undefined) {
      updates.push("delivery_url = ?");
      values.push(body.delivery_url);
    }
    if (body.verification_url !== undefined) {
      updates.push("verification_url = ?");
      values.push(body.verification_url);
    }
    if (body.status !== undefined) {
      updates.push("status = ?");
      values.push(body.status);
    }
    if (body.billing_url !== undefined) {
      updates.push("billing_url = ?");
      values.push(body.billing_url);
    }
    if (body.support_url !== undefined) {
      updates.push("support_url = ?");
      values.push(body.support_url);
    }
    if (body.delivery_url !== undefined) {
      updates.push("delivery_url = ?");
      values.push(body.delivery_url);
    }
    if (body.verification_url !== undefined) {
      updates.push("verification_url = ?");
      values.push(body.verification_url);
    }
    if (body.subtitle !== undefined) {
      updates.push("subtitle = ?");
      values.push(body.subtitle);
    }
    if (body.pricing !== undefined) {
      updates.push("pricing = ?");
      values.push(body.pricing);
    }
    if (body.vendor_notes !== undefined) {
      updates.push("vendor_notes = ?");
      values.push(body.vendor_notes);
    }
    if (body.tracer !== undefined) {
      updates.push("tracer = ?");
      values.push(body.tracer);
    }
    if (body.external_id !== undefined) {
      updates.push("external_id = ?");
      values.push(body.external_id);
      await updateExternalIDMapping(
        org_id,
        existingJobRun.external_id,
        body.external_id,
        jobRunId
      );
    }
    if (body.external_payload !== undefined) {
      updates.push("external_payload = ?");
      values.push(body.external_payload);
    }

    updates.push("updated_at = ?");
    values.push(now);
    updates.push("last_updated_at = ?");
    values.push(now); // Assuming last_updated_at is also updated

    if (
      updates.length === 2 &&
      updates.includes("updated_at = ?") &&
      updates.includes("last_updated_at = ?")
    ) {
      // Only contains timestamp updates
      // This means no user-provided fields are being updated.
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "No fields to update",
        })
      );
    }

    await dbHelpers.transaction("drive", org_id, (database) => {
      const stmt = database.prepare(
        `UPDATE job_runs SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values, jobRunId);

      // Handle labels
      if (body.labels !== undefined) {
        database
          .prepare(`DELETE FROM job_run_labels WHERE job_run_id = ?`)
          .run(jobRunId);
        if (body.labels.length > 0) {
          const insertLabelStmt = database.prepare(
            `INSERT INTO job_run_labels (job_run_id, label_id) VALUES (?, ?)`
          );
          for (const labelId of body.labels) {
            insertLabelStmt.run(jobRunId, labelId);
          }
        }
      }
    });

    const updatedJobRuns = await db.queryDrive(
      org_id,
      "SELECT * FROM job_runs WHERE id = ?",
      [jobRunId]
    );
    const updatedJobRun = updatedJobRuns[0] as JobRun;

    const permissionPreviews = isOwner
      ? [
          SystemPermissionType.CREATE,
          SystemPermissionType.EDIT,
          SystemPermissionType.DELETE,
          SystemPermissionType.VIEW,
          SystemPermissionType.INVITE,
        ]
      : await Promise.resolve().then(async () => {
          const recordPermissions = await checkSystemPermissionsService(
            `${jobRunId}` as SystemResourceID,
            requesterApiKey.user_id,
            org_id
          );
          const tablePermissions = await checkSystemPermissionsService(
            `TABLE_${SystemTableValueEnum.JOB_RUNS}` as SystemResourceID,
            requesterApiKey.user_id,
            org_id
          );
          return Array.from(
            new Set([...recordPermissions, ...tablePermissions])
          );
        });

    const jobRunFE: JobRunFE = {
      ...updatedJobRun,
      permission_previews: permissionPreviews,
    };

    const isVendorOfJob = requesterApiKey.user_id === updatedJobRun.vendor_id;
    const hasTableViewPermission = jobRunFE.permission_previews.includes(
      SystemPermissionType.VIEW
    );

    if (!isVendorOfJob && !hasTableViewPermission) {
      jobRunFE.notes = "";
    }
    if (!isVendorOfJob && !hasTableViewPermission) {
      jobRunFE.vendor_notes = "";
      jobRunFE.tracer = undefined;
    }

    const updateJobRunLabelsRaw = await db.queryDrive(
      org_id,
      `SELECT T2.value FROM job_run_labels AS T1 JOIN labels AS T2 ON T1.label_id = T2.id WHERE T1.job_run_id = ?`,
      [updatedJobRun.id]
    );
    jobRunFE.labels = (
      await Promise.all(
        updateJobRunLabelsRaw.map((row: any) =>
          redactLabelValue(org_id, row.value, requesterApiKey.user_id)
        )
      )
    ).filter((label): label is string => label !== null);

    jobRunFE.related_resources = []; // Ensure related_resources is empty

    return reply.status(200).send(createApiResponse(jobRunFE));
  } catch (error) {
    request.log.error("Error in updateJobRunHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function deleteJobRunHandler(
  request: FastifyRequest<{ Params: OrgIdParams; Body: IRequestDeleteJobRun }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const body = request.body;

    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const isOwner = requesterApiKey.user_id === (await getDriveOwnerId(org_id));

    const validation = validateDeleteJobRunRequest(body);
    if (!validation.valid) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: validation.error!,
        })
      );
    }

    const jobRunId = body.id;

    const hasDeletePermission = await Promise.resolve().then(async () => {
      const recordPermissions = await checkSystemPermissionsService(
        `${jobRunId}` as SystemResourceID,
        requesterApiKey.user_id,
        org_id
      );
      const tablePermissions = await checkSystemPermissionsService(
        `TABLE_${SystemTableValueEnum.JOB_RUNS}` as SystemResourceID,
        requesterApiKey.user_id,
        org_id
      );
      return (
        recordPermissions.includes(SystemPermissionType.DELETE) ||
        tablePermissions.includes(SystemPermissionType.DELETE)
      );
    });

    if (!isOwner && !hasDeletePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const jobRunsToDelete = await db.queryDrive(
      org_id,
      "SELECT external_id FROM job_runs WHERE id = ?",
      [jobRunId]
    );
    const externalIdToDelete =
      jobRunsToDelete.length > 0 ? jobRunsToDelete[0].external_id : null;

    await dbHelpers.transaction("drive", org_id, (database) => {
      database.prepare("DELETE FROM job_runs WHERE id = ?").run(jobRunId);
      database
        .prepare("DELETE FROM job_run_labels WHERE job_run_id = ?")
        .run(jobRunId);
      // Removed job_run_related_resources deletion
    });

    if (externalIdToDelete) {
      await updateExternalIDMapping(
        org_id,
        externalIdToDelete,
        undefined,
        jobRunId
      );
    }

    const deletedData: IResponseDeleteJobRun["ok"]["data"] = {
      id: jobRunId,
      deleted: true,
    };

    return reply.status(200).send(createApiResponse(deletedData));
  } catch (error) {
    request.log.error("Error in deleteJobRunHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

async function validateCreateJobRunRequest(
  body: IRequestCreateJobRun,
  orgID: DriveID
): Promise<{
  valid: boolean;
  error?: string;
}> {
  if (body.id) {
    if (!body.id.startsWith(IDPrefixEnum.JobRunID)) {
      return {
        valid: false,
        error: `JobRun ID must start with '${IDPrefixEnum.JobRunID}'.`,
      };
    }
    const alreadyClaimed = await isUUIDClaimed(orgID, body.id);
    if (alreadyClaimed) {
      return {
        valid: false,
        error: `Provided JobRun ID '${body.id}' is already claimed.`,
      };
    }
  }

  let validation: { valid: boolean; error?: string };

  if (body.vendor_name) {
    validation = validateShortString(body.vendor_name, "vendor_name");
    if (!validation.valid) return validation;
  }

  if (body.vendor_id) {
    validation = validateShortString(body.vendor_id, "vendor_id");
    if (!validation.valid) return validation;
  }

  if (body.vendor_notes) {
    validation = validateDescription(body.vendor_notes, "vendor_notes");
    if (!validation.valid) return validation;
  }

  if (body.vendor_id) {
    validation = { valid: validateIdString(body.vendor_id) };
    if (!validation.valid) return validation;
  }

  if (body.vendor_name) {
    validation = validateShortString(body.vendor_name, "vendor_name");
    if (!validation.valid) return validation;
  }

  if (body.description) {
    validation = validateDescription(body.description, "description");
    if (!validation.valid) return validation;
  }

  if (body.about_url) {
    const is_valid = validateUrl(body.about_url);
    if (!is_valid) return { valid: false, error: "about_url is required." };
  }

  if (body.title) {
    validation = validateShortString(body.title, "title");
    if (!validation.valid) return validation;
  }

  if (body.notes) {
    validation = validateDescription(body.notes, "notes");
    if (!validation.valid) return validation;
  }

  if (body.template_id) {
    validation = validateShortString(body.template_id, "template_id");
    if (!validation.valid) return validation;
  }

  if (body.vendor_id) {
    validation = validateShortString(body.vendor_id, "vendor_id");
    if (!validation.valid) return validation;
  }

  if (body.vendor_name) {
    validation = validateShortString(body.vendor_name, "vendor_name");
    if (!validation.valid) return validation;
  }

  if (body.vendor_notes) {
    validation = validateDescription(body.vendor_notes, "vendor_notes");
    if (!validation.valid) return validation;
  }

  if (body.support_url) {
    const is_valid = validateUrl(body.support_url);
    if (!is_valid) return { valid: false, error: "support_url is required." };
  }

  validation = validateShortString(body.title, "title");
  if (!validation.valid) return validation;

  if (body.notes) {
    validation = validateDescription(body.notes, "notes");
    if (!validation.valid) return validation;
  }

  if (body.about_url) {
    // about_url is mandatory but has validation.
    const is_valid = validateUrl(body.about_url);
    if (!is_valid) return { valid: false, error: "about_url is required." };
  }

  if (body.billing_url) {
    const is_valid = validateUrl(body.billing_url);
    if (!is_valid) return { valid: false, error: "billing_url is required." };
  }
  if (body.support_url) {
    const is_valid = validateUrl(body.support_url);
    if (!is_valid) return { valid: false, error: "support_url is required." };
  }
  if (body.delivery_url) {
    const is_valid = validateUrl(body.delivery_url);
    if (!is_valid) return { valid: false, error: "delivery_url is required." };
  }
  if (body.verification_url) {
    const is_valid = validateUrl(body.verification_url);
    if (!is_valid)
      return { valid: false, error: "verification_url is required." };
  }
  if (body.auth_installation_url) {
    const is_valid = validateUrl(body.auth_installation_url);
    if (!is_valid)
      return { valid: false, error: "auth_installation_url is required." };
  }

  if (body.subtitle) {
    const is_valid = validateShortString(body.subtitle, "subtitle");
    if (!is_valid) return { valid: false, error: "subtitle is required." };
  }
  if (body.pricing) {
    const is_valid = validateShortString(body.pricing, "pricing");
    if (!is_valid) return { valid: false, error: "pricing is required." };
  }
  if (body.next_delivery_date) {
    const is_valid = !isNaN(body.next_delivery_date);
    if (!is_valid)
      return { valid: false, error: "next_delivery_date must be a number." };
  }
  if (body.vendor_notes) {
    const is_valid = validateDescription(body.vendor_notes, "vendor_notes");
    if (!is_valid) return { valid: false, error: "vendor_notes is required." };
  }
  if (body.tracer) {
    const is_valid = validateShortString(body.tracer, "tracer");
    if (!is_valid) return { valid: false, error: "tracer is required." };
  }

  if (body.labels) {
    for (const label of body.labels) {
      const is_valid = validateShortString(label, "label");
      if (!is_valid) return { valid: false, error: "label is required." };
    }
  }

  if (body.external_id) {
    const is_valid = validateIdString(body.external_id);
    if (!is_valid) return { valid: false, error: "external_id is required." };
  }
  if (body.external_payload) {
    const is_valid = validateDescription(
      body.external_payload,
      "external_payload"
    );
    if (!validation.valid) return validation;
  }

  return { valid: true };
}

async function validateUpdateJobRunRequest(
  body: IRequestUpdateJobRun,
  orgID: DriveID
): Promise<{
  valid: boolean;
  error?: string;
}> {
  let is_valid = validateIdString(body.id);
  if (!is_valid) return { valid: false, error: "id is required." };

  if (body.billing_url) {
    is_valid = validateUrl(body.billing_url);
  }
  if (body.support_url) {
    is_valid = validateUrl(body.support_url);
    if (!is_valid) return { valid: false, error: "support_url is required." };
  }
  if (body.delivery_url) {
    is_valid = validateUrl(body.delivery_url);
    if (!is_valid) return { valid: false, error: "delivery_url is required." };
  }
  if (body.verification_url) {
    is_valid = validateUrl(body.verification_url);
    if (!is_valid)
      return { valid: false, error: "verification_url is required." };
  }

  if (body.subtitle) {
    is_valid = validateShortString(body.subtitle, "subtitle").valid;
    if (!is_valid) return { valid: false, error: "subtitle is required." };
  }
  if (body.pricing) {
    is_valid = validateShortString(body.pricing, "pricing").valid;
    if (!is_valid) return { valid: false, error: "pricing is required." };
  }
  if (body.next_delivery_date) {
    is_valid = !isNaN(body.next_delivery_date);
    if (!is_valid)
      return { valid: false, error: "next_delivery_date must be a number." };
  }
  if (body.vendor_notes) {
    is_valid = validateDescription(body.vendor_notes, "vendor_notes").valid;
    if (!is_valid) return { valid: false, error: "vendor_notes is required." };
  }
  if (body.tracer) {
    is_valid = validateShortString(body.tracer, "tracer").valid;
    if (!is_valid) return { valid: false, error: "tracer is required." };
  }

  if (body.labels) {
    for (const label of body.labels) {
      is_valid = validateShortString(label, "label").valid;
      if (!is_valid) return { valid: false, error: "label is required." };
    }
  }
  if (body.external_id) {
    is_valid = validateIdString(body.external_id);
    if (!is_valid) return { valid: false, error: "external_id is required." };
  }
  if (body.external_payload) {
    is_valid = validateDescription(
      body.external_payload,
      "external_payload"
    ).valid;
    if (!is_valid)
      return { valid: false, error: "external_payload is required." };
  }

  return { valid: true };
}

function validateDeleteJobRunRequest(body: IRequestDeleteJobRun): {
  valid: boolean;
  error?: string;
} {
  const is_valid = validateIdString(body.id);
  if (!is_valid) return { valid: false, error: "Invalid ID" };

  if (!body.id.startsWith(IDPrefixEnum.JobRunID)) {
    return {
      valid: false,
      error: `JobRun ID must start with '${IDPrefixEnum.JobRunID}'.`,
    };
  }
  return { valid: true };
}

function validateListJobRunsRequest(body: IRequestListJobRuns): {
  valid: boolean;
  error?: string;
} {
  if (body.filters && body.filters.length > 256) {
    return {
      valid: false,
      error: "Filters must be 256 characters or less",
    };
  }
  if (
    body.page_size !== undefined &&
    (body.page_size === 0 || body.page_size > 1000)
  ) {
    return {
      valid: false,
      error: "Page size must be between 1 and 1000",
    };
  }
  if (body.cursor && body.cursor.length > 256) {
    return { valid: false, error: "Cursor must be 256 characters or less" };
  }
  return { valid: true };
}
