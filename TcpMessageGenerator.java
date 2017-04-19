package com.symmetrics.collector.client;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.ResourceBundle;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.symmetrics.constants.CollectorConstants;
import com.symmetrics.constants.GeneratorConstants;
import com.symmetrics.utils.MessageUtils;
import com.symmetrics.utils.DateUtils;

public class TcpMessageGenerator {
	private static Logger logger = LogManager.getLogger(TcpMessageGenerator.class);
	private static String msgType = "";
	private static final long SKILLSET_SEED;
	private static final int AGENT_SEED;
	private static final int APPLICATION_SEED;
	private static final boolean SHOW_MSG;	
	static {
		ResourceBundle bundle = ResourceBundle.getBundle(GeneratorConstants.GENERATOR_PROPERTIES);
		SKILLSET_SEED = Integer.parseInt(bundle.getString(GeneratorConstants.RANDOM_SKILLSET_SEED));
		AGENT_SEED = Integer.parseInt(bundle.getString(GeneratorConstants.RANDOM_AGENT_SEED));
		APPLICATION_SEED = Integer.parseInt(bundle.getString(GeneratorConstants.RANDOM_APPLICATION_SEED));
		SHOW_MSG = Boolean.parseBoolean(bundle.getString(GeneratorConstants.TCP_GENERATOR_SHOW_MESSAGE));
	}

	public static void main(final String[] pArgs) throws Exception {
		if (pArgs.length != 6 && pArgs.length != 7) {
			logger.info("usage: tcpServer tcpPort msgType(agent,vdn,skill,agent_cumulative) "
					+ "tcpThreads tcpSleep, runWindow acd number (if 0, constant set)");
			return;
		}
		String tcpServer = pArgs[0];
		int tcpPort = Integer.parseInt(pArgs[1]);
		msgType = pArgs[2];
		int tcpThreads = Integer.parseInt(pArgs[3]);
		int tcpSleep = Integer.parseInt(pArgs[4]);
		int runWindow = Integer.parseInt(pArgs[5]) * 1000;
		int num = 0;
		if (pArgs.length == 7) {
			num = Integer.parseInt(pArgs[6]);
		}
		final AtomicBoolean writeRunning = new AtomicBoolean(true);
		final AtomicLong writeCounter = new AtomicLong(0);
		final ArrayList<Thread> writeThreads = new ArrayList<Thread>();
		logger.info("\n - TCP Port: " + tcpPort + "\n - Message: " + msgType + "\n - No. of Threads: " + tcpThreads
				+ "\n - Sleep duration: " + tcpSleep + "sec" + "\n - RunWindow: "
				+ (runWindow == 0 ? 0 : runWindow / 1000) + "sec" + "\n - ACD: " + "\n - No. of Records: " + num);
		int idx;
		for (idx = 0; idx < tcpThreads; idx++) {
			final Thread writeThread = new Thread(
					new Writer(writeRunning, writeCounter, idx, tcpServer, tcpPort, msgType, tcpSleep, num));
			logger.info("----- writer: writeRunning: " + writeRunning + " , writeCounter: " + writeCounter.get()
					+ " , idx: " + idx);
			writeThread.start();
			writeThreads.add(writeThread);
		}
		// run threads until sleep ends
		Thread.sleep(runWindow);
		writeRunning.set(false);
		logger.info("Run Window is up, Thread main will terminate (interrupt) " + writeThreads.size() + " thread(s) now!");
		for (final Thread writeThread : writeThreads) {
			writeThread.interrupt();
		}
	}
	/**
	 * The thread that is writing
	 */
	private static class Writer implements Runnable {
		private final AtomicBoolean _running;
		private final AtomicLong _counter;
		private int threadId;
		private int threadCounter;
		private String tcpServer;
		private int tcpPort;
		private int tcpSleep;
		private String tcpMsg;
		private int num;
		private boolean randomized = false;
		private Socket socket;

		@Override
		public void run() {
			final long counter = _counter.incrementAndGet();
			while (_running.get()) {
				logger.info("ThreadId: " + threadId + "  - Run?: " + _running.get());
				try {
					threadCounter = threadCounter + 1;
					logger.info("ThreadId: " + threadId + " is sending the tcp message now!");
					tcpMsg = getTcpMsg(msgType);
					if ((SHOW_MSG & randomized) || threadCounter == 1) {
						System.out.println(tcpMsg);
					}
					sendTcpMsg();
					logger.info(
							"ThreadId: " + threadId + " sent TCP messages, will sleep for " + tcpSleep + " seconds!");
					Thread.sleep(tcpSleep * 1000); // 1000 milliseconds is one
													// second.
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					logger.error("ThreadId: " + threadId + " is interrupted:" + ex.getMessage());
				} catch (Exception e) {
					logger.error("Thread exception:" + e.getMessage());
				}
			}
			logger.info("----- ThreadId:" + threadId + " , wrote: " + threadCounter + " time(s)," + " , _counter: "
					+ counter);
		}
		
		private Writer(final AtomicBoolean pRunning, final AtomicLong pCounter, int pThreadId, String pTcpServer,
				int pTcpPort, String pMsgType, int pTcpSleep, int pNum) {
			_running = pRunning;
			_counter = pCounter;
			threadId = pThreadId;
			threadCounter = 0;
			tcpServer = pTcpServer;
			tcpPort = pTcpPort;
			tcpSleep = pTcpSleep;
			num = pNum;
			if (num > 0)
				randomized = true;
			tcpMsg = getTcpMsg(msgType);
			socket = getTcpSocket();
		}
		
		public Socket getTcpSocket() {
			Socket s = new Socket();
			try {
				s = new Socket(tcpServer, tcpPort);
				s.setReuseAddress(true);
			} catch (Exception e) {
				logger.error(e.getMessage());
			} return s;
		}

		public void sendTcpMsg() throws Exception {
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			out.println(tcpMsg);			
		}
		
		public String getTcpMsg(String msgType) {
			String tcpMsg = "";
			// skillset
			if (msgType.equals(GeneratorConstants.GENERATOR_MSG_TYPE_SKILLSET)) {
				tcpMsg = skillsetMessageGen();
			//agent
			} else if (msgType.equals(GeneratorConstants.GENERATOR_MSG_TYPE_AGENT)) {
				tcpMsg = agentMessageGen();
			// application
			} else if (msgType.equals(GeneratorConstants.GENERATOR_MSG_TYPE_APPLICATION)) {
				tcpMsg = applicationMessageGen();
			} return tcpMsg;
		}
		
		private String agentMessageGen() {
			String tcpMsg = "";
			/*
			 * Agent Message Definition 
			 * 1 msg_type
			 * 2 msg_epoch
			 * 3 logid 
			 * 4 agentName 
			 * 5 stateNume
			 * 6 stateDesc
			 * 7 supervisorId
			 * 8 supervisorName
			 * 9 timeInState
			 * 10 ansSkillset
			 * 11 ansSkillsetDesc
			 * 12 dnInState
			 * 13 dnOutState
			 * 14 supervisorUserId
			 * 15 positionId
			 * 16 notReadyReason
			 * 17 notReadyReasonCode
			 * 18 dnOutCallNumber
			 * 19 skillsetCallsAnswered
			 * 20 dnInCallAnswered
			 * 21 dnOutCallMade
			 * 22 answeringApp
			 * 23 answeringCdn
			 * 24 answeringDnis
			 */
			// agent randomized msg
			if (randomized) {
				Long epoch = 0L;
				for (int i = 0; i < num; i++) {
					int agentID = (i + AGENT_SEED);
					String agentName = "agent".concat(String.valueOf(agentID));
					DateTime tzNow = new DateTime(DateTimeZone.forID(CollectorConstants.BASE_TIMEZONE));			
					epoch = DateUtils.getCurrentEpoch(tzNow);
					int stateNum = MessageUtils.randomInteger(1000, 9000);
					String stateDesc = "state".concat(String.valueOf(stateNum));
					int timeInState = MessageUtils.randomInteger(0,6000);
					int ansSkillset = MessageUtils.randomInteger(0, 15);
					String ansSkillsetDesc = "skillset"+ansSkillset;
					int notReadyReasonCode = MessageUtils.randomInteger(0,20);
					String notRdyRsn = "notReadyReason"+notReadyReasonCode;
					int skillsetCallsAnswered = MessageUtils.randomInteger(0, 50);
					int dnInCallsAns = MessageUtils.randomInteger(0, 45);
					int dnOutCallsMade = MessageUtils.randomInteger(0, 50);
					if(i == (num-1))
						tcpMsg = tcpMsg +  "agent|"+epoch+"|"+agentID+"|"+agentName+"|"+stateNum+"|"+stateDesc+"|0|0|"+timeInState+"|"+ansSkillset+"|"+ansSkillsetDesc+"|0|0|0|0|"+notRdyRsn+"|"+notReadyReasonCode+"|0|"+skillsetCallsAnswered+"|"+dnInCallsAns+"|"+dnOutCallsMade+"|0|0|0";
					else
						tcpMsg = tcpMsg +  "agent|"+epoch+"|"+agentID+"|"+agentName+"|"+stateNum+"|"+stateDesc+"|0|0|"+timeInState+"|"+ansSkillset+"|"+ansSkillsetDesc+"|0|0|0|0|"+notRdyRsn+"|"+notReadyReasonCode+"|0|"+skillsetCallsAnswered+"|"+dnInCallsAns+"|"+dnOutCallsMade+"|0|0|0\n";
				}
			} else {
				DateTime tzNow = new DateTime(DateTimeZone.forID(CollectorConstants.BASE_TIMEZONE));
				Long epoch = DateUtils.getCurrentEpoch(tzNow);
				// 1
				tcpMsg = tcpMsg +  "agent|"+epoch+"|100|agent100|10|state 10|0|0|100|2|skillset 2|0|0|0|0|Not_Ready_Reason|5|0|13|23|10|0|0|0\n";
				// 2
				tcpMsg = tcpMsg +  "agent|"+epoch+"|101|agent101|10|state 10|0|0|100|2|skillset 2|0|0|0|0|Not_Ready_Reason|5|0|13|23|10|0|0|0\n";
				//3
				tcpMsg = tcpMsg +  "agent|"+epoch+"|102|agent102|10|state 10|0|0|100|2|skillset 2|0|0|0|0|Not_Ready_Reason|5|0|13|23|10|0|0|0\n";
				//4
				tcpMsg = tcpMsg +  "agent|"+epoch+"|103|agent103|10|state 10|0|0|100|2|skillset 2|0|0|0|0|Not_Ready_Reason|5|0|13|23|10|0|0|0\n";
				//5
				tcpMsg = tcpMsg +  "agent|"+epoch+"|104|agent104|10|state 10|0|0|100|2|skillset 2|0|0|0|0|Not_Ready_Reason|5|0|13|23|10|0|0|0";
			}
			//tcpMsg = tcpMsg + "==EOD==";
			return tcpMsg;
		}

		private String skillsetMessageGen() {
		String tcpMsg = "";
		/*
		Skillset Message Definition:
		1	msg_type
		2	msg_epoch
		3	SkillsetID --
		4	SkillsetName
		5	AgentsAvailable --
		6	AgentsInService --
		7	AgentsonSkillsetCalls
		8	AgentsNotReady --
		9	CallsWaiting --
		10	LongestWaitingTimeSinceLastCall
		11	Max.WaitingTime --
		12	WaitingTime 
		13	ExpectedWaitTimE
		14	CallsAnsweredAfterThreshold --
		15	LongestWaitingTimeSinceLogin
		16	AgentsonDNCalls --
		17	SkillsetState --
		18	AgentsUnavailable --
		19	NetworkCallsWaiting
		20	NetworkCallsAnswered
		21	TotalCallsAnsweredDelay --
		22	TotalCallsAnswered --
		23	AgentOnNetworkSkillsetCall
		24	AgentOnOtherSkillsetCall --
		25	AgentOnACD-DNCall --
		26	AgentOnNACD-DNCall --
		27	CallsOffered --
		28	NetworkCallsOffered
		29	SkillsetAbandon --
		30	SkillsetAbandonDelay --
		31	SkillsetAbandonDelayAfterThreshold --
		32	QueuedCallAnswered
		 */
		if (randomized) {
			Long epoch = 0L;
			for (int i = 0; i < num; i++) {
				long SkillsetID = (i + SKILLSET_SEED);
				DateTime tzNow = new DateTime(DateTimeZone.forID(CollectorConstants.BASE_TIMEZONE));			
				epoch = DateUtils.getCurrentEpoch(tzNow);
				int msgAgentsAvailable = MessageUtils.randomInteger(0, 15);
				int msgAgentsInService = MessageUtils.randomInteger(0, 15);
				int msgAgentsNotReady =  MessageUtils.randomInteger(0, 10);
				int CallsWaiting =  MessageUtils.randomInteger(0, 10);
				int MaxWaitingTime =  MessageUtils.randomInteger(0, 60000);
				int msgCallsAnsweredAfterThreshold = MessageUtils.randomInteger(0, 1000);
				int msgAgentsonDNCalls = MessageUtils.randomInteger(0, 10);
				int msgSkillsetState = MessageUtils.randomInteger(0, 10);
				int msgAgentsUnavailable = MessageUtils.randomInteger(0, 10);
				int msgTotalCallsAnsweredDelay = MessageUtils.randomInteger(0, 25);
				int msgTotalCallsAnswered = MessageUtils.randomInteger(0, 25);
				int msgAgentOnOtherSkillsetCall =  MessageUtils.randomInteger(0, 5);
				int msgAgentOnACDDNCall =  MessageUtils.randomInteger(0, 8);
				int msgAgentOnNACDDNCall =  MessageUtils.randomInteger(0, 8);
				int msgCallsOffered =  MessageUtils.randomInteger(0, 50);
				int msgSkillsetAbandon =  MessageUtils.randomInteger(0, 10);
				int msgSkillsetAbandonDelay =  MessageUtils.randomInteger(0, 10);
				int msgSkillsetAbandonDelayAfterThreshold =  MessageUtils.randomInteger(0, 10);
				if(i== (num-1))
					tcpMsg = tcpMsg + "skillset|"+epoch+"|"+SkillsetID+"|MT_Canyon_Door_sk|"+msgAgentsAvailable+"|"+msgAgentsInService+"|0|"+msgAgentsNotReady + "|"+CallsWaiting+"|0|"+MaxWaitingTime+"|0|0|"+msgCallsAnsweredAfterThreshold+"|0|"+msgAgentsonDNCalls+"|"+msgSkillsetState+"|"+msgAgentsUnavailable+"|0|0|"+msgTotalCallsAnsweredDelay+"|"+msgTotalCallsAnswered+"|0|"+msgAgentOnOtherSkillsetCall+"|"+msgAgentOnACDDNCall+"|"+msgAgentOnNACDDNCall+"|"+msgCallsOffered+"|0|"+msgSkillsetAbandon+"|"+msgSkillsetAbandonDelay+"|"+msgSkillsetAbandonDelayAfterThreshold+"|0";
				else
					tcpMsg = tcpMsg + "skillset|"+epoch+"|"+SkillsetID+"|MT_Canyon_Door_sk|"+msgAgentsAvailable+"|"+msgAgentsInService+"|0|"+msgAgentsNotReady + "|"+CallsWaiting+"|0|"+MaxWaitingTime+"|0|0|"+msgCallsAnsweredAfterThreshold+"|0|"+msgAgentsonDNCalls+"|"+msgSkillsetState+"|"+msgAgentsUnavailable+"|0|0|"+msgTotalCallsAnsweredDelay+"|"+msgTotalCallsAnswered+"|0|"+msgAgentOnOtherSkillsetCall+"|"+msgAgentOnACDDNCall+"|"+msgAgentOnNACDDNCall+"|"+msgCallsOffered+"|0|"+msgSkillsetAbandon+"|"+msgSkillsetAbandonDelay+"|"+msgSkillsetAbandonDelayAfterThreshold+"|0\n";
			}
		} else {
			DateTime tzNow = new DateTime(DateTimeZone.forID(CollectorConstants.BASE_TIMEZONE));
			Long epoch = DateUtils.getCurrentEpoch(tzNow);
			// 1
			tcpMsg = tcpMsg + "skillset|"+epoch+"|100|MT_Canyon_Door_sk|10|2|0|3|100|0|35467|0|0|25|0|12|2|3|0|0|2|10|0|1|11|1|32|0|4|5|2|0\n";
			//2
			tcpMsg = tcpMsg + "skillset|"+epoch+"|101|MT_Canyon_Door_sk|10|2|0|3|100|0|35467|0|0|25|0|12|1|3|0|0|2|10|0|1|11|1|32|0|4|5|2|0\n";
			//3
			tcpMsg = tcpMsg + "skillset|"+epoch+"|102|MT_Canyon_Door_sk|10|2|0|3|100|0|35467|0|0|25|0|12|1|3|0|0|2|10|0|1|11|1|32|0|4|5|2|0\n";
			//4
			tcpMsg = tcpMsg + "skillset|"+epoch+"|103|MT_Canyon_Door_sk|10|2|0|3|100|0|35467|0|0|25|0|12|1|3|0|0|2|10|0|1|11|1|32|0|4|5|2|0\n";
			//4
			tcpMsg = tcpMsg + "skillset|"+epoch+"|104|MT_Canyon_Door_sk|10|2|0|3|100|0|35467|0|0|25|0|12|1|3|0|0|2|10|0|1|11|1|32|0|4|5|2|0\n";
			//5
			tcpMsg = tcpMsg + "skillset|"+epoch+"|105|MT_Canyon_Door_sk|10|2|0|3|100|0|35467|0|0|25|0|12|1|3|0|0|2|10|0|1|11|1|32|0|4|5|2|0";
			} return tcpMsg;
		}

		private String applicationMessageGen() {
			String tcpMsg = "";
			/* Agent Message Definition 
			 * 1 msg_type 
			 * 2 epoch
			 * 3 applicationId 	--					
			 * 4 applicationName	--				
			 * 5 callsAbandoned		--				 
			 * 6 callsAbandonedAfterThreshold --			
			 * 7 callsAbandonedDelay --				
			 * 8 callsAnswered	 --					 	
			 * 9 callsAnsweredAfterThreshold --		
			 * 10 callsAnsweredDelay --					
			 * 11 callsWaiting	--					 
			 * 12 maxWaitingTime --				
			 * 13 waitingTime	--					
			 * 14 callsAnsweredDelayAtSkillset
			 * 15 callsGivenTerminationTreatment
			 * 16 callsOffered	--					
			 * 17 timeBeforeInterflow
			 * 18 networkOutCalls
			 * 19 networkOutCallsAbandon
			 * 20 networkOutCallsAbandonDelay
			 * 21 networkOutCallsAnswered
			 * 22 networkOutCallsAnsweredDelay
			 * 23 networkOutCallsWaiting
			 * 24 networkOutCallsRequested
			 */
			
			// application randomized msg
			if (randomized) {
				Long epoch = 0L;
				for (int i = 0; i < num; i++) {
					int appId = (i + APPLICATION_SEED);
					DateTime tzNow = new DateTime(DateTimeZone.forID(CollectorConstants.BASE_TIMEZONE));			
					epoch = DateUtils.getCurrentEpoch(tzNow);
					String appName = "app"+appId;
					int callsAban = MessageUtils.randomInteger(0, 100);
					int callsAbanAfterThresh = MessageUtils.randomInteger(0, 10);
					int callsAbanDelay = MessageUtils.randomInteger(0, 100);
					int callsAns = MessageUtils.randomInteger(0, 30);
					int callsAnsAfterThresh = MessageUtils.randomInteger(0, 10);
					int callsAnsDelay = MessageUtils.randomInteger(0, 40);
					int maxWaitTime = MessageUtils.randomInteger(1000,6000);
					int waitTime = MessageUtils.randomInteger(0, 1000);
					int callsWaiting = MessageUtils.randomInteger(0, 30);
					int callsOffered = MessageUtils.randomInteger(0, 30);
					if(i== (num-1))
						tcpMsg = tcpMsg +  "application|"+epoch+"|"+appId+"|"+appName+"|"+callsAban+"|"+callsAbanAfterThresh+"|"+callsAbanDelay+"|"+callsAns+"|"+callsAnsAfterThresh+"|"+callsAnsDelay+"|"+callsWaiting+"|"+maxWaitTime+"|"+waitTime+"|0|0|"+callsOffered+"|0|0|0|0|0|0|0|0";
					else
						tcpMsg = tcpMsg +  "application|"+epoch+"|"+appId+"|"+appName+"|"+callsAban+"|"+callsAbanAfterThresh+"|"+callsAbanDelay+"|"+callsAns+"|"+callsAnsAfterThresh+"|"+callsAnsDelay+"|"+callsWaiting+"|"+maxWaitTime+"|"+waitTime+"|0|0|"+callsOffered+"|0|0|0|0|0|0|0|0\n";
				}
			} else {
				DateTime tzNow = new DateTime(DateTimeZone.forID(CollectorConstants.BASE_TIMEZONE));
				Long epoch = DateUtils.getCurrentEpoch(tzNow);
				// 1
				tcpMsg = tcpMsg +  "application|"+epoch+"|100|app 100|2|3|4|10|10|1|13|45620|345|0|0|20|0|0|0|0|0|0|0|0\n";
				// 2
				tcpMsg = tcpMsg +  "application|"+epoch+"|101|app 100|2|3|4|10|10|1|13|45620|345|0|0|20|0|0|0|0|0|0|0|0\n";
				// 3
				tcpMsg = tcpMsg +  "application|"+epoch+"|102|app 100|2|3|4|10|10|1|13|45620|345|0|0|20|0|0|0|0|0|0|0|0\n";
				// 4
				tcpMsg = tcpMsg +  "application|"+epoch+"|103|app 100|2|3|4|10|10|1|13|45620|345|0|0|20|0|0|0|0|0|0|0|0\n";
				// 5
				tcpMsg = tcpMsg +  "application|"+epoch+"|104|app 100|2|3|4|10|10|1|13|45620|345|0|0|20|0|0|0|0|0|0|0|0";
							
			} return tcpMsg;
		}
	}
}
