/****** Object:  Table [dbo].[msg_send]    Script Date: 08/03/2006 09:27:14 AM ******/
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[msg_send]') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
drop table [dbo].[msg_send]
GO

/****** Object:  Table [dbo].[msg_send]    Script Date: 08/03/2006 09:27:16 AM ******/
CREATE TABLE [dbo].[msg_send] (
	[id] [int] IDENTITY (1, 1) NOT NULL ,
	[type] [char] (1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[created] [datetime] NULL ,
	[status] [char] (1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[updated] [datetime] NULL ,
	[errorcode] [char] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[errormsg] [text] COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[patient_id] [char] (15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[patient_lastname] [char] (20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[patient_firstname] [char] (15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[patient_middle] [char] (15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[patient_dob] [datetime] NULL ,
	[patient_sex] [char] (1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[patient_status] [char] (1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[visit_id] [char] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[visit_accession] [char] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[visit_scheduled] [datetime] NULL ,
	[visit_requestor] [char] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[visit_exam] [char] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[visit_char1] [char] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[visit_char2] [char] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[visit_char3] [char] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[visit_char4] [char] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL ,
	[report] [text] COLLATE SQL_Latin1_General_CP1_CI_AS NULL 
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

