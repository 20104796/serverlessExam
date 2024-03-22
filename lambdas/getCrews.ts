import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
    try {


        const parameters = event?.pathParameters;
        const crewRole = parameters?.role ? parameters.role: undefined;
        const movieId = parameters?.movieId ? parseInt(parameters.movieId) : undefined;


        let commandInput = {
            TableName: process.env.TABLE_NAME,
            KeyConditionExpression: "movieId = :movieId and crewRole = :crewRole",
            ExpressionAttributeValues: {
                ":movieId": movieId,
                ":crewRole": crewRole,
            },

        };

        const queryParams = event.queryStringParameters;
        const names = queryParams?.names ? queryParams.names: undefined;
        if (names) {
            let commandInput = {
                TableName: process.env.TABLE_NAME,
                KeyConditionExpression: "movieId = :movieId and crewRole = :crewRole and begins_with(names, :names)",
                ExpressionAttributeValues: {
                    ":movieId": movieId,
                    ":crewRole": crewRole,
                    ":names": names,
                },

            };
        }

        const commandOutput = await ddbDocClient.send(new QueryCommand(commandInput));

        if (!commandOutput.Items || commandOutput.Items.length === 0) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ message: "No crew found for the given criteria" }),
            };
        }

        const body = {
            data: commandOutput.Items
        };
        return {
            statusCode: 200,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify(body),
        };
    } catch (error: any) {
        console.log(JSON.stringify(error));
        return {
            statusCode: 500,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({ error }),
        };
    }
};

function createDDbDocClient() {
    const ddbClient = new DynamoDBClient({ region: process.env.REGION });
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = { marshallOptions, unmarshallOptions };
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
