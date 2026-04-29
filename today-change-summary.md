# Numat AI Sales Assistant: Summary of Recent Changes

This note summarizes the main changes made in the latest round of work, starting from the point where CRM activity was split between sales outreach and customer services.

## 1. CRM activity is now separated into two categories

CRM emails are now treated as one of two types:

- `Sales Outreach`
- `Customer Services`

The purpose of this change is to keep all customer communication visible in the system, while making sure that generic service traffic does not distort sales follow-up decisions.

### What counts as Customer Services

Customer service/admin-style traffic is now identified more deliberately, including:

- anything sent to or from `customerservice@numatsystems.com`
- invoice-related emails
- purchase order / accounts payable communication
- shipping document traffic
- automatic replies
- apology / correction notes such as `Please disregard`

### Why this matters

This means that invoicing, shipping, and general admin traffic can stay in the CRM history, but it will not be mistaken for active sales engagement.

## 2. Sales follow-up logic now uses Sales Outreach only

The following areas now use `Sales Outreach` only when deciding whether a customer is actively being worked:

- `Today’s Action Plan`
- `Hold / Recently Contacted`
- `Customers Needing Attention`
- `Already contacted recently` badges
- customer and summary `Last Activity` fields

This gives the reps a much cleaner view of genuine sales contact.

## 3. CRM filters were added to the main CRM views

New CRM category filters were added to:

- `CRM Activities`
- the customer `CRM Timeline`

Users can now filter those views by:

- `All`
- `Sales Outreach`
- `Customer Services`

This makes it easier to review the full customer story while still separating operational/service traffic from active sales communication.

## 4. Customer CRM timeline was improved visually

Each CRM timeline row now shows:

- `Sales Outreach` or `Customer Services`
- `Inbound` or `Outbound`

These badges are stacked so the category is shown first and the direction second, which makes the timeline easier to scan.

## 5. Home page action plan was simplified

The home page was changed from a split East/West action plan into a single prioritized list.

### Current setup

- one `Today’s Action Plan`
- top 10 due today
- `Hold / Recently Contacted` remains separate

The title was also made larger and centered, and the description line underneath was removed for a cleaner presentation.

## 6. Urgency dismissals were added

Reps can now dismiss a customer from urgency-driven views when there is a known operational reason that follow-up is not required at that moment.

### How it works

- each action-plan item can be dismissed
- a short dismissal reason can be added
- dismissed items are removed from:
  - `Today’s Action Plan`
  - `Customers Needing Attention`
- dismissed items can be restored later
- the dismissal is tied to the current `Last Order`, so it naturally stops applying if a new order is placed

This was designed to avoid permanently hiding customers while still letting reps suppress false alarms.

## 7. Outreach Prep was added to the action plan

A new `Prepare outreach` button was added to each action-plan item.

This opens a dedicated `Outreach Prep` view for that customer.

## 8. AI Outreach Prep: first phase is now live

The first phase of the AI outreach feature has been built.

### What it currently provides

- recommended outreach mode
- tone guidance
- confidence signal
- short rationale
- evidence summary
- draft email subject
- draft email body
- call objective
- call talking points
- suggested voicemail
- suggested text message

This is currently a review/prep tool only. It does not send anything yet.

## 9. Business context was added to improve AI quality

A business context document was added so the AI understands what Numat does and how to interpret the communication history properly.

This includes context such as:

- Numat repairs damaged mats for industrial laundries
- `damaged mats` usually means a repair opportunity, not a complaint
- customer service/admin traffic should not be treated as the main sales signal
- some replies indicate genuine commercial blockers rather than generic customer issues

This improved the quality of recommendations significantly, especially for accounts where repair-service conversations could otherwise be misunderstood.

## 10. The most recent sales outreach is now weighted more strongly

The outreach-prep logic now treats the latest sales outreach item as a high-priority context signal.

This is especially helpful for:

- visit summaries
- meeting follow-ups
- post-visit notes

The AI is now more likely to reference the latest meaningful sales interaction rather than drifting into a generic follow-up recommendation.

## 11. Recent sales context was cleaned up

The `Recent Sales Outreach Context` section now:

- shows true recent mixed sales history
- includes both inbound and outbound sales communication
- excludes customer service/admin noise more reliably

A separate `Latest Replied Outreach` section is also available to highlight a proven response-backed interaction.

## 12. Contact inference has been added to Outreach Prep

The system now looks at the customer’s sales history and tries to infer the most relevant contacts.

### What it does

- detects named contacts and email addresses from sales history
- identifies the top 2–3 most relevant contacts
- infers a light-touch role/influence signal, such as:
  - `Operational contact`
  - `Decision path / blocker`
  - `Finance / admin`
  - `Active responder`
  - `Unknown role`

### Where this is shown

On the `Outreach Prep` page under:

- `Likely Sales Contacts`

The AI also uses this inferred contact information to decide:

- who the best target is
- how the tone should be adapted

## 13. Target contact is now part of the outreach recommendation

The outreach recommendation now includes:

- `Target Contact`
- a `Targeting note`

This helps reps understand who the draft is really aimed at and why.

## 14. Current state

At this point, the system now does a much better job of:

- separating service/admin traffic from genuine sales activity
- prioritizing who needs follow-up
- letting reps suppress false urgency
- preparing better AI-assisted outreach
- grounding those drafts in real customer history and contact behavior

## 15. Suggested next steps

The most likely next areas for refinement are:

1. review the outreach-prep output with reps and tune any weak recommendations
2. expand the contact-role logic if the team finds it helpful
3. build the fuller outreach `scorecard`
4. later, add edit/send/logging when the reps are happy with the AI prep quality
