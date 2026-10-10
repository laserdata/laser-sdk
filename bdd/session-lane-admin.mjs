import { Laser, TopicRetention } from "../foreign/typescript/dist/index.js";

const [endpoint, stream, change, countText, username] = process.argv.slice(2);
const count = Number(countText);
if (
  endpoint === undefined ||
  stream === undefined ||
  !Number.isInteger(count) ||
  count < 1
)
  throw new Error(
    "lane mutation needs an endpoint, stream, change and partition count",
  );
const laser = await Laser.connectWithStream(endpoint, stream);
try {
  switch (change) {
    case "writer credentials": {
      if (username === undefined)
        throw new Error("writer fixture needs a username");
      const details = await laser.client.stream.get({ streamId: stream });
      const topic = await laser.client.topic.get({
        streamId: stream,
        topicId: "agent.sessions",
      });
      if (details === null || topic === null)
        throw new Error("the provisioned session lane is missing");
      await laser.client.user.create({
        username,
        password: "lane-writer-test",
        status: 1,
        permissions: {
          global: {
            ManageServers: false,
            ReadServers: true,
            ManageUsers: false,
            ReadUsers: false,
            ManageStreams: false,
            ReadStreams: false,
            ManageTopics: false,
            ReadTopics: false,
            PollMessages: false,
            SendMessages: false,
          },
          streams: [
            {
              streamId: details.id,
              permissions: {
                manageStream: false,
                readStream: true,
                manageTopics: false,
                readTopics: true,
                pollMessages: false,
                sendMessages: false,
              },
              topics: [
                {
                  topicId: topic.id,
                  permissions: {
                    manage: false,
                    read: true,
                    pollMessages: true,
                    sendMessages: true,
                  },
                },
              ],
            },
          ],
        },
      });
      break;
    }
    case "partition count":
      await laser.client.partition.create({
        streamId: stream,
        topicId: "agent.sessions",
        partitionCount: 1,
      });
      break;
    case "topic generation":
      await laser.client.topic.delete({
        streamId: stream,
        topicId: "agent.sessions",
        partitionsCount: count,
      });
      await laser.topic("agent.sessions").ensure(count);
      break;
    case "stream generation":
      await laser.stream(stream).delete();
      await laser.stream(stream).ensure();
      await laser.bootstrap(count, TopicRetention.expireAfter(86400000));
      break;
    default:
      throw new Error(`unknown lane mutation ${change}`);
  }
} finally {
  await laser.close();
}
