import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const s3Client = new S3Client();
const dynamoClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const snsClient = new SNSClient();

const TABLE_NAME = "Submissions";
const SNS_TOPIC_ARN = "arn:aws:sns:eu-north-1:629017097497:submission-topic";
const DEADLINE = "2026-04-25";

export const handler = async (event) => {
    try {
        const record = event.Records[0];
        const bucket = record.s3.bucket.name;
        const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

        // Get file from S3
        const getCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
        const file = await s3Client.send(getCommand);
        const body = await streamToString(file.Body);
        const data = JSON.parse(body);

        let errors = [];
        const requiredFields = ["studentId", "name", "assignmentId", "submissionDate", "answers"];
        requiredFields.forEach(field => {
            if (!data[field]) errors.push(`Missing ${field}`);
        });

        let isLate = false;
        if (data.submissionDate > DEADLINE) {
            isLate = true;
            errors.push("Late submission");
        }

        const status = errors.length === 0 ? "VALID" : "INVALID";

        const result = {
            studentId: data.studentId || "unknown",
            assignmentId: data.assignmentId || "unknown",
            status: status,
            late: isLate,
            errors: errors,
            timestamp: new Date().toISOString()
        };

        // Save to DynamoDB
        const putCommand = new PutCommand({
            TableName: TABLE_NAME,
            Item: result
        });
        await docClient.send(putCommand);

        // Send SNS notification
        const publishCommand = new PublishCommand({
            TopicArn: SNS_TOPIC_ARN,
            Message: JSON.stringify(result, null, 2),
            Subject: "Submission Result"
        });
        await snsClient.send(publishCommand);

        console.log("Success:", result);
        return result;
    } catch (error) {
        console.error("Error:", error);
        throw error;
    }
};

// Helper to convert stream to string
async function streamToString(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
        stream.on("error", reject);
    });
}
