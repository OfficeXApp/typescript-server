import { FastifyRequest, FastifyReply } from "fastify";
import { v4 as uuidv4 } from "uuid";
import {
  Drive,
  DriveFE,
  DriveID,
  ICPPrincipalString,
  IDPrefixEnum,
  IRequestCreateDrive,
  IRequestDeleteDrive,
  IRequestGetDrive,
  IRequestListDrives,
  IRequestUpdateDrive,
  IResponseDeleteDrive,
  IPaginatedResponse,
  SystemPermissionType,
  SystemTableValueEnum,
  URLEndpoint,
  UserID,
  ISuccessResponse,
  SortDirection,
  SystemResourceID,
} from "@officexapp/types";
import { db, dbHelpers } from "../../../../services/database";
import { authenticateRequest } from "../../../../services/auth";
import { createApiResponse, getDriveOwnerId, OrgIdParams } from "../../types";

// Import the actual permission checking functions from the services/permissions/system.ts
import {
  checkSystemPermissions,
  redactLabelValue,
} from "../../../../services/permissions/system";

import { validateIcpPrincipal } from "../../../../services/validation";
import {
  claimUUID,
  isUUIDClaimed,
  updateExternalIDMapping,
} from "../../../../services/external";

// Helper to cast Drive to DriveFE and apply redaction logic
async function castDriveToFE(
  drive: Drive,
  userId: UserID,
  orgId: DriveID
): Promise<DriveFE> {
  const isOwner = userId === (await getDriveOwnerId(orgId));

  const permissionPreviews = await checkSystemPermissions({
    resourceTable: `TABLE_${SystemTableValueEnum.DRIVES}`,
    resourceId: `${drive.id}` as SystemResourceID,
    granteeId: userId,
    orgId: orgId,
  });

  let privateNote = drive.private_note;
  // Rust's SystemPermission.cast_fe does not redact private_note based on permission_previews.
  // It relies on is_owner. If private_note redaction is tied to EDIT permission here, it's an enhancement.
  // For now, aligning with Rust's stricter redaction for private_note based solely on ownership or a direct "can access record" check.
  // The provided Rust code for DirectoryPermission::cast_fe also doesn't seem to redact private_note based on permissions.
  // It seems the `redacted` function in Rust only applies to labels and external_id/payload.
  // Assuming `private_note` is only accessible to the owner based on general patterns.
  if (!isOwner) {
    privateNote = undefined;
  }

  // Apply redaction to labels using the imported redactLabelValue
  const redactedLabels = (
    await Promise.all(
      (drive.labels || []).map(
        async (label) => await redactLabelValue(orgId, label, userId)
      )
    )
  ).filter((label: any) => label !== null) as string[];

  return {
    ...drive,
    private_note: privateNote,
    labels: redactedLabels,
    permission_previews: permissionPreviews,
  };
}

// Request params for GetDriveHandler
interface GetDriveParams extends OrgIdParams {
  drive_id: string; // This is the drive ID passed in the URL path, not the tenant org_id
}

export async function getDriveHandler(
  request: FastifyRequest<{ Params: GetDriveParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id as DriveID
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const requestedDriveId = request.params.drive_id as DriveID;
    const orgId = request.params.org_id as DriveID; // The tenant's drive ID
    const requesterUserId = requesterApiKey.user_id;

    const isOwner = requesterUserId === (await getDriveOwnerId(orgId));

    let drive: Drive | null = null;
    try {
      const result = await db.queryDrive(
        orgId,
        "SELECT * FROM drives WHERE id = ?",
        [requestedDriveId]
      );
      if (result && result.length > 0) {
        drive = result[0] as Drive;
      }
    } catch (e) {
      request.log.error(
        `Error querying drive ${requestedDriveId} for org ${orgId}:`,
        e
      );
      return reply.status(500).send(
        createApiResponse(undefined, {
          code: 500,
          message: "Internal server error querying drive",
        })
      );
    }

    if (!drive) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Drive not found",
        })
      );
    }

    // Use the imported canUserAccessSystemPermission to check if the user can view this drive record.
    // const canViewDrive = await _canUserAccessSystemPermission(
    //   requestedDriveId, // Resource ID (e.g., "DriveID_xyz")
    //   requesterUserId,
    //   orgId
    // );

    // if (!canViewDrive) {
    //   return reply
    //     .status(403)
    //     .send(
    //       createApiResponse(undefined, { code: 403, message: "Forbidden" })
    //     );
    // }

    const driveFE = await castDriveToFE(drive, requesterUserId, orgId);
    return reply.status(200).send(createApiResponse(driveFE));
  } catch (error) {
    request.log.error("Error in getDriveHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function listDrivesHandler(
  request: FastifyRequest<{ Body: IRequestListDrives; Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const { org_id } = request.params;
    const requestBody = request.body;

    // 1. Authenticate the request
    const requesterApiKey = await authenticateRequest(request, "drive", org_id);
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }
    const requesterUserId = requesterApiKey.user_id;

    // 2. Validate request body
    const pageSize = requestBody.page_size || 50;
    if (pageSize === 0 || pageSize > 1000) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: page_size - Page size must be between 1 and 1000",
        })
      );
    }
    if (requestBody.filters && requestBody.filters.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: filters - Filters must be 256 characters or less",
        })
      );
    }
    if (requestBody.cursor && requestBody.cursor.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message:
            "Validation error: cursor - Cursor must be 256 characters or less",
        })
      );
    }

    // 3. Construct the dynamic SQL query
    const direction = requestBody.direction || SortDirection.DESC;
    const filters = requestBody.filters || "";

    let query = `SELECT * FROM drives`;
    const queryParams: any[] = [];
    const whereClauses: string[] = [];

    if (filters) {
      // Assuming a simple filter on the drive's name
      whereClauses.push(`(name LIKE ?)`);
      queryParams.push(`%${filters}%`);
    }

    if (whereClauses.length > 0) {
      query += ` WHERE ${whereClauses.join(" AND ")}`;
    }

    query += ` ORDER BY created_at ${direction}`;

    let offset = 0;
    if (requestBody.cursor) {
      offset = parseInt(requestBody.cursor, 10);
      if (isNaN(offset)) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: "Invalid cursor format",
          })
        );
      }
    }

    query += ` LIMIT ? OFFSET ?`;
    queryParams.push(pageSize, offset);

    // 4. Execute the query
    const rawDrives = await db.queryDrive(org_id, query, queryParams);

    // 5. Process and filter drives based on permissions
    const processedDrives: DriveFE[] = [];
    for (const drive of rawDrives) {
      const driveRecordResourceId: string = `${drive.id}`;

      const permissions = await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.DRIVES}`,
        resourceId: driveRecordResourceId,
        granteeId: requesterUserId,
        orgId: org_id,
      });

      if (permissions.includes(SystemPermissionType.VIEW)) {
        processedDrives.push(
          await castDriveToFE(drive as Drive, requesterUserId, org_id)
        );
      }
    }

    // 6. Handle pagination and total count
    const nextCursor =
      processedDrives.length < pageSize ? null : (offset + pageSize).toString();

    const totalCountToReturn = processedDrives.length;

    return reply.status(200).send(
      createApiResponse<IPaginatedResponse<DriveFE>>({
        items: processedDrives,
        page_size: processedDrives.length,
        total: totalCountToReturn,
        direction: direction,
        cursor: nextCursor || undefined,
      })
    );
  } catch (error) {
    request.log.error("Error in listDrivesHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function createDriveHandler(
  request: FastifyRequest<{ Body: IRequestCreateDrive; Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id as DriveID
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const orgId = request.params.org_id as DriveID;
    const requesterUserId = requesterApiKey.user_id;

    // Check for CREATE permission on the DRIVES table
    const hasCreatePermission = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.DRIVES}`,
        granteeId: requesterApiKey.user_id,
        orgId: orgId,
      })
    ).includes(SystemPermissionType.CREATE);

    const isOwner = requesterUserId === (await getDriveOwnerId(orgId));
    if (!isOwner && !hasCreatePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    const {
      id,
      name,
      icp_principal,
      public_note,
      private_note,
      endpoint_url,
      external_id,
      external_payload,
    } = request.body;

    // Validate request body
    if (id) {
      if (!id.startsWith(IDPrefixEnum.Drive)) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: `Drive ID must start with '${IDPrefixEnum.Drive}'.`,
          })
        );
      }
      // Check if the provided ID is already claimed (Rust's `validate_unclaimed_uuid` check)
      const alreadyClaimed = await isUUIDClaimed(orgId, id);
      if (alreadyClaimed) {
        return reply.status(400).send(
          createApiResponse(undefined, {
            code: 400,
            message: `Provided Drive ID '${id}' is already claimed.`,
          })
        );
      }
    }
    if (!name || name.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Name is required and must be less than 256 characters",
        })
      );
    }
    if (!icp_principal) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "ICP Principal is required",
        })
      );
    }
    const icpPrincipalValidation = validateIcpPrincipal(icp_principal);
    if (!icpPrincipalValidation.success) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: icpPrincipalValidation.error.message,
        })
      );
    }
    if (public_note && public_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Public note too long",
        })
      );
    }
    if (private_note && private_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Private note too long",
        })
      );
    }
    if (endpoint_url && endpoint_url.length > 4096) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Endpoint URL too long",
        })
      );
    }
    if (external_id && external_id.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "External ID too long",
        })
      );
    }
    if (external_payload && external_payload.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "External payload too long",
        })
      );
    }

    const driveId: DriveID = (id ||
      `${IDPrefixEnum.Drive}${uuidv4()}`) as DriveID;
    const createdAt = Date.now(); // Milliseconds

    const newDrive: Drive = {
      id: driveId,
      name: name,
      icp_principal: icp_principal as ICPPrincipalString,
      public_note: public_note,
      private_note: private_note,
      endpoint_url: (endpoint_url ||
        `https://${icp_principal}.icp0.io`) as URLEndpoint, // Default endpoint
      last_indexed_ms: undefined,
      labels: [], // No labels on creation by default
      external_id: external_id,
      external_payload: external_payload,
      created_at: createdAt,
    };

    // FIX: dbHelpers.transaction is async
    await dbHelpers.transaction("drive", orgId, async (database) => {
      // Insert into drives table
      const stmt = database.prepare(
        `INSERT INTO drives (id, name, icp_principal, public_note, private_note, endpoint_url, last_indexed_ms, created_at, external_id, external_payload)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      );
      stmt.run(
        newDrive.id,
        newDrive.name,
        newDrive.icp_principal,
        newDrive.public_note,
        newDrive.private_note,
        newDrive.endpoint_url,
        newDrive.last_indexed_ms,
        newDrive.created_at,
        newDrive.external_id,
        newDrive.external_payload
      );

      if (newDrive.external_id) {
        // Update external ID mapping as per Rust's `update_external_id_mapping`
        await updateExternalIDMapping(
          orgId,
          undefined, // No old external ID for creation
          newDrive.external_id,
          newDrive.id
        );
      }

      // Mark the generated/provided DriveID as claimed in the `uuid_claimed` table.
      const successfullyClaimed = await claimUUID(orgId, newDrive.id);
      if (!successfullyClaimed) {
        // This should ideally not be reached if validation is perfect, but provides a safeguard.
        throw new Error(
          `Failed to claim UUID for new drive '${newDrive.id}'. It might have been claimed concurrently.`
        );
      }
    });

    const driveFE = await castDriveToFE(newDrive, requesterUserId, orgId);
    return reply.status(200).send(createApiResponse(driveFE));
  } catch (error) {
    request.log.error("Error in createDriveHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function updateDriveHandler(
  request: FastifyRequest<{ Body: IRequestUpdateDrive; Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id as DriveID
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const orgId = request.params.org_id as DriveID;
    const requesterUserId = requesterApiKey.user_id;

    const {
      id,
      name,
      public_note,
      private_note,
      endpoint_url,
      external_id,
      external_payload,
    } = request.body;

    // Validate request body
    if (!id || !id.startsWith(IDPrefixEnum.Drive)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Drive ID is required and must start with DriveID_",
        })
      );
    }
    if (name && name.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Name must be less than 256 characters",
        })
      );
    }
    if (public_note && public_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Public note too long",
        })
      );
    }
    if (private_note && private_note.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Private note too long",
        })
      );
    }
    if (endpoint_url && endpoint_url.length > 4096) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Endpoint URL too long",
        })
      );
    }
    if (external_id && external_id.length > 256) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "External ID too long",
        })
      );
    }
    if (external_payload && external_payload.length > 8192) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "External payload too long",
        })
      );
    }

    const existingDrives = await db.queryDrive(
      orgId,
      "SELECT * FROM drives WHERE id = ?",
      [id]
    );

    if (!existingDrives || existingDrives.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Drive not found",
        })
      );
    }

    const currentDrive = existingDrives[0] as Drive;

    // Check for EDIT permission on this specific drive record
    const hasEditPermission = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.DRIVES}`,
        resourceId: `${currentDrive.id}` as SystemResourceID,
        granteeId: requesterUserId,
        orgId: orgId,
      })
    ).includes(SystemPermissionType.EDIT);

    const isOwner = requesterUserId === (await getDriveOwnerId(orgId));
    if (!isOwner && !hasEditPermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    // Build update query dynamically
    const updates: string[] = [];
    const values: any[] = [];
    let oldExternalId: string | undefined;

    if (name !== undefined) {
      updates.push("name = ?");
      values.push(name);
    }
    if (public_note !== undefined) {
      updates.push("public_note = ?");
      values.push(public_note);
    }
    if (private_note !== undefined) {
      updates.push("private_note = ?");
      values.push(private_note);
    }
    if (endpoint_url !== undefined) {
      updates.push("endpoint_url = ?");
      values.push(endpoint_url);
    }
    if (external_id !== undefined) {
      oldExternalId = currentDrive.external_id; // Capture old external_id
      updates.push("external_id = ?");
      values.push(external_id);
    }
    if (external_payload !== undefined) {
      updates.push("external_payload = ?");
      values.push(external_payload);
    }

    if (updates.length === 0) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "No fields to update",
        })
      );
    }

    values.push(id); // The ID for the WHERE clause

    // FIX: dbHelpers.transaction is async
    await dbHelpers.transaction("drive", orgId, async (database) => {
      const stmt = database.prepare(
        `UPDATE drives SET ${updates.join(", ")} WHERE id = ?`
      );
      stmt.run(...values);

      if (external_id !== undefined || oldExternalId !== undefined) {
        // Check if `external_id` was involved in the update
        await updateExternalIDMapping(
          orgId,
          oldExternalId, // The external_id before the update
          external_id, // The new external_id from the request body
          id // The internal DriveID
        );
      }
    });

    const updatedDrives = await db.queryDrive(
      orgId,
      "SELECT * FROM drives WHERE id = ?",
      [id]
    );

    const updatedDrive = updatedDrives[0] as Drive;
    const updatedDriveFE = await castDriveToFE(
      updatedDrive,
      requesterUserId,
      orgId
    );

    return reply.status(200).send(createApiResponse(updatedDriveFE));
  } catch (error) {
    request.log.error("Error in updateDriveHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}

export async function deleteDriveHandler(
  request: FastifyRequest<{ Body: IRequestDeleteDrive; Params: OrgIdParams }>,
  reply: FastifyReply
): Promise<void> {
  try {
    const requesterApiKey = await authenticateRequest(
      request,
      "drive",
      request.params.org_id as DriveID
    );
    if (!requesterApiKey) {
      return reply
        .status(401)
        .send(
          createApiResponse(undefined, { code: 401, message: "Unauthorized" })
        );
    }

    const orgId = request.params.org_id as DriveID;
    const requesterUserId = requesterApiKey.user_id;

    const { id } = request.body;

    // Validate request body
    if (!id || !id.startsWith(IDPrefixEnum.Drive)) {
      return reply.status(400).send(
        createApiResponse(undefined, {
          code: 400,
          message: "Drive ID is required and must start with DriveID_",
        })
      );
    }

    const existingDrives = await db.queryDrive(
      orgId,
      "SELECT * FROM drives WHERE id = ?",
      [id]
    );

    if (!existingDrives || existingDrives.length === 0) {
      return reply.status(404).send(
        createApiResponse(undefined, {
          code: 404,
          message: "Drive not found",
        })
      );
    }

    // Check for DELETE permission on this specific drive record
    const hasDeletePermission = (
      await checkSystemPermissions({
        resourceTable: `TABLE_${SystemTableValueEnum.DRIVES}`,
        resourceId: `${id}` as SystemResourceID,
        granteeId: requesterUserId,
        orgId: orgId,
      })
    ).includes(SystemPermissionType.DELETE);

    const isOwner = requesterUserId === (await getDriveOwnerId(orgId));
    if (!isOwner && !hasDeletePermission) {
      return reply
        .status(403)
        .send(
          createApiResponse(undefined, { code: 403, message: "Forbidden" })
        );
    }

    await dbHelpers.transaction("drive", orgId, (database) => {
      const stmt = database.prepare("DELETE FROM drives WHERE id = ?");
      stmt.run(id);

      // TODO: UUID Handle deletion from external ID mapping if a separate table exists
      // If `external_id` was stored in a separate mapping table, delete corresponding entries here.
    });

    const deletedData: IResponseDeleteDrive["ok"]["data"] = {
      id: id as DriveID,
      deleted: true,
    };

    return reply.status(200).send(createApiResponse(deletedData));
  } catch (error) {
    request.log.error("Error in deleteDriveHandler:", error);
    return reply.status(500).send(
      createApiResponse(undefined, {
        code: 500,
        message: `Internal server error - ${error}`,
      })
    );
  }
}
