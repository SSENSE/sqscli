package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// service struct embeds the sqs connector
// @TODO - maybe create a "Queue" type that encapsulates queue metadata !
type service struct {
	*sqs.SQS
}

func init() {
	// Go / no go ?
	help := flag.Bool("help", false, "help")
	flag.BoolVar(help, "h", false, "help") // Aliasing
	flag.Parse()

	if len(os.Args) == 1 || *help {
		usage()
	}
}

func main() {
	// Subcommands
	toCsvCommand := flag.NewFlagSet("qtocsv", flag.ExitOnError)

	// Flags
	queueName := toCsvCommand.String("queue", "", "queue name")
	toCsvCommand.StringVar(queueName, "q", "", "queue name") // Aliasing
	queueHelp := toCsvCommand.Bool("help", false, "help for qtocsv command")
	toCsvCommand.BoolVar(queueHelp, "h", false, "help") // Aliasing

	// Command
	switch os.Args[1] {
	case "qtocsv":
		toCsvCommand.Parse(os.Args[2:])
		if *queueHelp {
			toCSVUsage()
		}
		toCSV(*queueName)
	default:
		fmt.Println("Command not found.")
	}
}

// - - - - - - - - - - - - - - - -
//   COMMANDS
// - - - - - - - - - - - - - - - -

// toCSV outputs the content of a queue in a CSV file
func toCSV(queue string) {
	// Verify
	if len(queue) == 0 {
		fmt.Println("Required queue name is missing.")
		toCSVUsage()
	}

	// Connect
	svc := newService()

	// Query the queue
	qURL := svc.getQueueURL(queue)
	fifo := svc.isFIFO(qURL)
	var readdMessages []*sqs.Message // Messages to re-add later

	insertCSVHead(fifo)
	// Getting all messages
	for {
		result := svc.receiveMessages(qURL, 10, fifo) // Batch of 10

		if len(result.Messages) == 0 {
			break // We are done
		}

		// Process
		for _, m := range result.Messages {
			// Readd later
			readdMessages = append(readdMessages, m)
			formatCSV(m, fifo)
		}

		// Delete in batch
		svc.deleteMessageBatch(qURL, result.Messages)
	}

	// Re-add the messages to the queue
	errs := svc.sendMessageBatch(qURL, readdMessages, 10, fifo)
	if len(errs) > 0 {
		log.Fatal("There were errors re-adding the messages", errs)
	}
}

// - - - - - - - - - - - - - - - -
//   COMMANDS HELPERS
// - - - - - - - - - - - - - - - -

// insertCSVHead adds row header to the CSV output
func insertCSVHead(fifo bool) {
	if fifo {
		fmt.Println("Body,Message Group ID,Message Deduplication ID,Sequence Number,Sent")
	} else {
		fmt.Println("Body,Sent")
	}
}

// formatCSV outputs a CSV formatted row
func formatCSV(m *sqs.Message, fifo bool) {
	// Remove spaces
	mess := strings.Join(strings.Fields(*m.Body), " ")
	// Escape double quotes
	mess = strings.Replace(mess, "\"", "\\\"", -1)

	if fifo {
		fmt.Printf("%s,%s,%s,%s,%s\n",
			mess,
			*m.Attributes["MessageGroupId"],
			*m.Attributes["MessageDeduplicationId"],
			*m.Attributes["SequenceNumber"],
			*m.Attributes["SentTimestamp"])
	} else {
		fmt.Printf("\"%s\",\"%s\"\n",
			mess,
			*m.Attributes["SentTimestamp"])
	}
}

// - - - - - - - - - - - - - - - -
//   MANIPULATING QUEUES
// - - - - - - - - - - - - - - - -

// newService returns a SQS connection
func newService() *service {
	// Get environment variables
	keyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if keyID == "" || secretKey == "" {
		log.Fatal("Missing connection credentials")
	}
	// Connect
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: credentials.NewStaticCredentials(keyID, secretKey, ""),
	})
	if err != nil {
		log.Fatal("Error connecting to AWS ", err)
	}
	svc := sqs.New(sess)
	return &service{svc}
}

// getQueueURL returns the FQDN for a queue name
func (s *service) getQueueURL(name string) string {
	queueInfo, err := s.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		log.Fatalf("Error finding queue %s: %s\n", name, err)
	}
	return *queueInfo.QueueUrl
}

// getQueueAttributes returns metadata for a queue url
func (s *service) getQueueAttributes(queue string) *sqs.GetQueueAttributesOutput {
	attr, err := s.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queue),
		AttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
	})
	if err != nil {
		log.Fatalf("Error fetching queue attributes %s: %s\n", queue, err)
	}
	return attr
}

// receiveMessages fetches SQS messages in batches
func (s *service) receiveMessages(queue string, num int, fifo bool) *sqs.ReceiveMessageOutput {
	// @TODO - use worker pools to fetch faster
	messageInput := &sqs.ReceiveMessageInput{
		QueueUrl: &queue,
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		MaxNumberOfMessages: aws.Int64(int64(num)),
		VisibilityTimeout:   aws.Int64(10), // 10 seconds
		WaitTimeSeconds:     aws.Int64(0),
	}

	if fifo {
		messageInput.AttributeNames = []*string{aws.String(sqs.QueueAttributeNameAll)}
	}

	result, err := s.ReceiveMessage(messageInput)

	if err != nil {
		log.Fatal("Error fetching message ", err)
	}

	return result
}

// sendMessageBatch pushes SQS messages in a queue
// for performance reasons we have a FIFO argument
func (s *service) sendMessageBatch(queue string, messages []*sqs.Message, batch int, fifo bool) []error {

	var entries []*sqs.SendMessageBatchRequestEntry
	var errors []error

	// For each Batches
	for i := 0; i < len(messages); i += batch {
		j := i + batch
		if j > len(messages) {
			j = len(messages)
		}
		// Prepare payload
		entries = nil
		for _, m := range messages[i:j] {
			//uuid, _ := newUUID()
			d := sqs.SendMessageBatchRequestEntry{
				MessageAttributes: map[string]*sqs.MessageAttributeValue{
					"SentTimestamp": &sqs.MessageAttributeValue{
						DataType:    aws.String("String"),
						StringValue: aws.String(*m.Attributes["SentTimestamp"]),
					},
				},
				Id:          aws.String(*m.MessageId),
				MessageBody: aws.String(*m.Body),
			}
			getBatchRequestEntryAttributes(&d, m, fifo)
			entries = append(entries, &d)
		}

		messageInput := &sqs.SendMessageBatchInput{
			Entries:  entries,
			QueueUrl: aws.String(queue),
		}

		_, err := s.SendMessageBatch(messageInput)
		if err != nil {
			// We couldn't readd the messages
			// this is bad because it means we will lose the message(s)
			// still we need to continue in order not to lose more messages
			errors = append(errors, err)
		}
	}
	return errors
}

// sendMessage pushes a SQS message in a queue
// for performance reasons we have a FIFO argument
func (s *service) sendMessage(queue string, message *sqs.Message, fifo bool) {
	messageInput := &sqs.SendMessageInput{
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"SentTimestamp": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(*message.Attributes["SentTimestamp"]),
			},
		},
		MessageBody: aws.String(*message.Body),
		QueueUrl:    &queue,
	}

	// FIFO ?
	if fifo {
		// Preparing Deduplication ID
		uuid, _ := newUUID()
		messageInput.MessageDeduplicationId = aws.String(string(uuid))
		messageInput.MessageGroupId = aws.String(*message.Attributes["MessageGroupId"])
		messageInput.MessageAttributes["SequenceNumber"] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(*message.Attributes["SequenceNumber"]),
		}
		messageInput.MessageAttributes["MessageGroupId"] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(*message.Attributes["MessageGroupId"]),
		}
		messageInput.MessageAttributes["SenderId"] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(*message.Attributes["SenderId"]),
		}
		messageInput.MessageAttributes["ApproximateFirstReceiveTimestamp"] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(*message.Attributes["ApproximateFirstReceiveTimestamp"]),
		}
		messageInput.MessageAttributes["ApproximateReceiveCount"] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(*message.Attributes["ApproximateReceiveCount"]),
		}
	} else {
		messageInput.DelaySeconds = aws.Int64(1)
	}

	_, err := s.SendMessage(messageInput)

	if err != nil {
		log.Fatal("Error sending message", err)
	}
}

// deleteMessageBatch deletes a batch of messages from a queue
func (s *service) deleteMessageBatch(queue string, messages []*sqs.Message) {
	// Prepare payload
	var entries []*sqs.DeleteMessageBatchRequestEntry
	for _, m := range messages {
		entry := &sqs.DeleteMessageBatchRequestEntry{Id: m.MessageId, ReceiptHandle: m.ReceiptHandle}
		entries = append(entries, entry)
	}
	// Batch ready
	batchInput := sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(queue),
	}

	_, err := s.DeleteMessageBatch(&batchInput)
	// @TODO - re-run errors - or not
	// an error just means the message was not deleted and will be fetched on the next iteration (FIFO)
	// for non-FIFO queues messages are processed one by one anyway
	if err != nil {
		fmt.Println("Delete Error", err)
		// os.Exit(1)
	}
}

// deleteMessage deletes a message from a queue
func (s *service) deleteMessage(queue string, message *sqs.Message) {
	_, err := s.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &queue,
		ReceiptHandle: message.ReceiptHandle,
	})

	if err != nil {
		log.Fatal("Delete Error", err)
	}
}

// isFIFO is true if the queue is a FIFO, else otherwise
// this is an expensive operation, store the returned boolean in a variable
func (s *service) isFIFO(queue string) bool {
	attr := s.getQueueAttributes(queue)

	if attr.Attributes["FifoQueue"] == nil {
		return false
	}

	b, err := strconv.ParseBool(*attr.Attributes["FifoQueue"])
	if err != nil {
		log.Fatal("Error determining queue type", err)
	}
	return b
}

// getBatchRequestEntryAttributes is a helper function for sendMessageBatch
func getBatchRequestEntryAttributes(req *sqs.SendMessageBatchRequestEntry, m *sqs.Message, fifo bool) {
	// FIFO ?
	if fifo {
		// Preparing Deduplication ID
		uuid, _ := newUUID()
		req.MessageDeduplicationId = aws.String(string(uuid))
		req.MessageGroupId = aws.String(*m.Attributes["MessageGroupId"])
		req.MessageAttributes["SequenceNumber"] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(*m.Attributes["SequenceNumber"]),
		}
		req.MessageAttributes["MessageGroupId"] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(*m.Attributes["MessageGroupId"]),
		}
		req.MessageAttributes["SenderId"] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(*m.Attributes["SenderId"]),
		}
		req.MessageAttributes["ApproximateFirstReceiveTimestamp"] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(*m.Attributes["ApproximateFirstReceiveTimestamp"]),
		}
		req.MessageAttributes["ApproximateReceiveCount"] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(*m.Attributes["ApproximateReceiveCount"]),
		}
	} else {
		req.DelaySeconds = aws.Int64(1)
	}
}

// - - - - - - - - - - - - - - - -
//   UTILS
// - - - - - - - - - - - - - - - -

// newUUID generates a pseudo-random UUID
// used for Deduplication ID in FIFO queues
func newUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// variant bits
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random)
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x%x%x%x%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

// - - - - - - - - - - - - - - - -
//   USAGE OUTPUT
// - - - - - - - - - - - - - - - -

func usage() {
	fmt.Println("usage: sqscli <command> [<args>]")
	fmt.Println("The most commonly used sqscli commands are: ")
	fmt.Println(" qtocsv   Output a queue in a csv format")
	fmt.Println(" blablabla  Send stuff")
	os.Exit(0)
}

func toCSVUsage() {
	fmt.Println("usage: sqscli qtocsv [options]")
	fmt.Println("options:")
	fmt.Println("  -queue required   Queue name")
	os.Exit(0)
}
